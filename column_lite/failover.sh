#!/bin/sh
# failover.sh — ПК пропал -> колонка живёт на своём lite; ПК вернулся -> снова на ПК.
#
# Зачем: когда ПК включён, колонку обслуживает quasard на компьютере (голос,
# будильники). Когда ПК выключен — качать музыку/радио/книги некому, и колонка
# должна сама перейти на локальный quasard-lite (127.0.0.1:80/443).
#
# Переключение делается двумя вещами:
#   1) DNS-пины /etc/unbound/unbound.conf: на ПК или на себя (127.0.0.1);
#   2) iptables REDIRECT $PC:80/443 -> локальные 80/443 в lite-режиме —
#      страховка от DNS-кэша колонки (она держит старый IP и ходит на него).
# /etc/unbound — tmpfs (rw), правится без remount. unbound перезапускается.
#
#   sh failover.sh start   # запустить сторож (из S99litewatch при загрузке)
#   sh failover.sh once    # один цикл проверки
#   sh failover.sh stop
set -u
CFG=/data/opt/cfg
# IP компьютера-сервера quasard в вашей сети. В репозитории адреса нет:
# его задаёт deploy_lite.sh --pc-ip (файл $CFG/pc_ip) или переменная PC_IP.
# Пусто — живём в lite без маршрута на ПК (голос-реле и ADB/SSH с ПК выкл.).
PC="${PC_IP:-$(cat "$CFG/pc_ip" 2>/dev/null || true)}"
case "$PC" in *[!0-9.]* | "") PC="";; esac
U=/etc/unbound/unbound.conf
LITE=/data/opt/oleg/S99zlite
LOCK=/data/quasar/lock_column_egress.sh
RUN=/tmp/oleg
mkdir -p "$RUN"
STATE="$RUN/failover.mode"     # pc | lite
LOG="$RUN/failover.log"

log() { echo "$(date '+%m-%d %H:%M:%S') $*" >> "$LOG"; }

# Порвать застрявшее conntrack-отображение микрофонного потока ($PC:5600).
# Пакет, попадающий в существующую запись, проходит мимо nat-OUTPUT и до DNAT не
# доходит. Сбрасываем двусторонне: raw-DROP держим RAW_HOLD секунд — дольше
# nf_conntrack_udp_timeout (30 с) в состоянии UNREPLIED (иначе запись переживёт
# сброс, и поток так и останется мимо DNAT). Без дублей: перед вставкой -C.
# Проверено 18.11 на железе: удержание 35 с вернуло микрофон (0 -> 12646 пакетов).
RAW_HOLD=40
drop_stale_conntrack() {
    iptables -t raw -C OUTPUT -d "$PC" -p udp --dport 5600 -j DROP 2>/dev/null \
      || iptables -t raw -I OUTPUT 1 -d "$PC" -p udp --dport 5600 -j DROP
    ( sleep "$RAW_HOLD"; while iptables -t raw -C OUTPUT -d "$PC" -p udp --dport 5600 -j DROP 2>/dev/null; do
        iptables -t raw -D OUTPUT -d "$PC" -p udp --dport 5600 -j DROP
      done ) &
}

pc_alive() {
    # ВАЖНО: только ping. Раньше проба шла HTTP-запросом на $PC:80/443, а это
    # в lite-режиме REDIRECT-ом уводилось в НАШ ЖЕ lite — сторож принимал себя
    # за живой ПК и скакал между режимами (цикл pc->lite->pc). ICMP редиректом
    # не трогается, egress-файрвол LAN пускает.
    # АВТОНОМНЫЙ РЕЖИМ ПО УМОЛЧАНИЮ (решение 17.11): колонка живёт только на
    # своём lite, ПК нужен лишь для отладки по ADB. Старое поведение («ПК жив ->
    # обслуживает ПК») включается флагом: touch /data/opt/cfg/use_pc
    [ -f "$CFG/use_pc" ] || return 1
    [ -n "$PC" ] || return 1
    ping -c 1 -W 2 "$PC" >/dev/null 2>&1
}

write_pins() {  # $1 = pc|lite
    if [ "$1" = lite ]; then src="$CFG/unbound.lite"; else src="$CFG/unbound.oleg"; fi
    [ -f "$src" ] || { log "нет $src"; return 1; }
    cp "$src" "$U" && sync
    /etc/init.d/S70unbound restart >/dev/null 2>&1
}

nat_on() {
    # Адрес ПК неизвестен — маршрутизировать на него нечего: lite работает
    # полностью автономно (DNS-пины на себя, голос через DNAT ставится в
    # ensure_mic_nat только при известном PC).
    [ -n "$PC" ] || { log "pc_ip не задан — NAT на ПК пропущен"; return 0; }
    iptables -t nat -C OUTPUT -d "$PC" -p tcp --dport 80  -j REDIRECT --to-ports 80  2>/dev/null \
      || iptables -t nat -I OUTPUT 1 -d "$PC" -p tcp --dport 80  -j REDIRECT --to-ports 80
    iptables -t nat -C OUTPUT -d "$PC" -p tcp --dport 443 -j REDIRECT --to-ports 443 2>/dev/null \
      || iptables -t nat -I OUTPUT 1 -d "$PC" -p tcp --dport 443 -j REDIRECT --to-ports 443
    # чистим следы ручных проб: DROP на PC:443 ломал связь в pc-режиме
    while iptables -C OUTPUT -d "$PC" -p tcp --dport 443 -j DROP 2>/dev/null; do
        iptables -D OUTPUT -d "$PC" -p tcp --dport 443 -j DROP
    done
    # Голос без ПК: профиль «Олег» шлёт PCM микрофона на $PC:5600. ПК мёртв —
    # заворачиваем этот UDP на себя, где его принимает VoiceLoop в lite,
    # распознаёт (бесплатный движок) и отдаёт команду колонке.
    # ВАЖНО: DNAT на СВОЙ LAN-адрес, а не REDIRECT на 127.0.0.1 — при
    # route_localnet=0 (так на прошивке) ядро молча отбрасывает loopback-NAT,
    # и пакеты пропадают (проверено: счётчик редиректа рос, а VoiceLoop — 0).
    SELF_IP=$(ip -4 addr show wlan0 2>/dev/null | grep -o 'inet [0-9.]*' | head -1 | cut -d' ' -f2)
    if [ -n "${SELF_IP:-}" ]; then
        iptables -t nat -C OUTPUT -d "$PC" -p udp --dport 5600 -j DNAT --to-destination "$SELF_IP:5600" 2>/dev/null \
          || iptables -t nat -I OUTPUT 1 -d "$PC" -p udp --dport 5600 -j DNAT --to-destination "$SELF_IP:5600"
        # Устаревшая запись conntrack держит старый маршрут mic-потока: правила
        # сменились, а пакеты «Олега» продолжают идти мимо DNAT (счётчик правила
        # стоит на нуле, VoiceLoop получает 0 пакетов). (Проверено 18.11: на
        # колонке так висел поток — DNAT=0, а 215k пакетов уходили в DROP.)
        # Держать raw-DROP нужно ДОЛЬШЕ таймаута UDP-conntrack в состоянии
        # UNREPLIED (nf_conntrack_udp_timeout=30 с) и уж точно дольше
        # старого 8-секундного окна: иначе запись переживает сброс. Фоновый
        # процесс кладёт правило, спит 40 с и снимает — поток за это время
        # пересоздаётся и попадает в DNAT.
        drop_stale_conntrack
    fi
    # старый (нерабочий) REDIRECT на loopback убираем, чтобы не мешал
    while iptables -t nat -C OUTPUT -d "$PC" -p udp --dport 5600 -j REDIRECT --to-ports 5600 2>/dev/null; do
        iptables -t nat -D OUTPUT -d "$PC" -p udp --dport 5600 -j REDIRECT --to-ports 5600
    done
}

nat_off() {
    [ -n "$PC" ] || return 0
    while iptables -t nat -C OUTPUT -d "$PC" -p tcp --dport 80 -j REDIRECT --to-ports 80 2>/dev/null; do
        iptables -t nat -D OUTPUT -d "$PC" -p tcp --dport 80 -j REDIRECT --to-ports 80
    done
    while iptables -t nat -C OUTPUT -d "$PC" -p tcp --dport 443 -j REDIRECT --to-ports 443 2>/dev/null; do
        iptables -t nat -D OUTPUT -d "$PC" -p tcp --dport 443 -j REDIRECT --to-ports 443
    done
    while iptables -C OUTPUT -d "$PC" -p tcp --dport 443 -j DROP 2>/dev/null; do
        iptables -D OUTPUT -d "$PC" -p tcp --dport 443 -j DROP
    done
    # микрофон снова идёт на живой ПК (там профиль «Олег» его и распознаёт)
    while iptables -t nat -C OUTPUT -d "$PC" -p udp --dport 5600 -j REDIRECT --to-ports 5600 2>/dev/null; do
        iptables -t nat -D OUTPUT -d "$PC" -p udp --dport 5600 -j REDIRECT --to-ports 5600
    done
    # DNAT удаляем по номеру: у --to-destination адрес меняется, точное совпадение
    # писать не получится. ВАЖНО про awk: в выводе iptables цель («DNAT») стоит
    # в НАЧАЛЕ строки, а dpt:5600 — дальше, поэтому порядок в шаблоне любой.
    while :; do
        n=$(iptables -t nat -L OUTPUT -n --line-numbers 2>/dev/null \
            | awk '/dpt:5600/ && /DNAT/ {print $1; exit}')
        [ -z "$n" ] && break
        iptables -t nat -D OUTPUT "$n" || break
    done
}

ensure_lite() {
    if [ -f /tmp/quasard-lite.pid ] && kill -0 "$(cat /tmp/quasard-lite.pid)" 2>/dev/null; then
        return 0
    fi
    sh "$LITE" start >/dev/null 2>&1
}

to_lite() {
    log "ПК недоступен -> lite"
    ensure_lite
    [ -x "$LOCK" ] && sh "$LOCK" >/dev/null 2>&1
    nat_on
    write_pins lite
    echo lite > "$STATE"
}

to_pc() {
    log "ПК на связи -> pc"
    nat_off
    write_pins pc
    echo pc > "$STATE"
}

# В lite-режиме DNAT на микрофон должен существовать ВСЕГДА. Если to_lite
# отработал раньше, чем wlan0 получил IP, правило не создалось — и никогда
# не появится само (to_lite больше не вызывается, пока режим lite). Смена
# IP по DHCP тоже оставляет DNAT на старый адрес. Проверяем дёшево и
# переставляем только при необходимости.
ensure_mic_nat() {
    [ -f "$CFG/use_pc" ] && return 0
    [ -n "$PC" ] || return 0
    SELF_IP=$(ip -4 addr show wlan0 2>/dev/null | grep -o 'inet [0-9.]*' | head -1 | cut -d' ' -f2)
    [ -n "${SELF_IP:-}" ] || return 0
    want="DNAT.*to:$SELF_IP:5600"
    if ! iptables -t nat -L OUTPUT -n 2>/dev/null | grep -q "$want"; then
        # DNAT с другим адресом (сменился IP) — сносим все и ставим заново
        while :; do
            n=$(iptables -t nat -L OUTPUT -n --line-numbers 2>/dev/null \
                | awk '/dpt:5600/ && /DNAT/ {print $1; exit}')
            [ -z "$n" ] && break
            iptables -t nat -D OUTPUT "$n" || break
        done
        iptables -t nat -I OUTPUT 1 -d "$PC" -p udp --dport 5600 -j DNAT --to-destination "$SELF_IP:5600"
        log "DNAT микрофона переставлен на $SELF_IP"
        drop_stale_conntrack
    fi
}

cur_mode() { [ -f "$STATE" ] && cat "$STATE" || echo ""; }

# Вход из LAN: SSH :2222 (root без пароля!) и ADB :5555 открывались всей сети —
# в гостях рядом стоит чужой роутер/камера/ТВ, а с нашей колонки можно было
# получить shell. Открываем их только ПК (для отладки), остальным — DROP.
# Глагол :1961 и микрофонный UDP :5600 снаружи не нужны вовсе: lite ходит в
# glagol по 127.0.0.1, микрофон приходит через DNAT на себя (аудит 19.11/20.11).
# Повторяем в каждом цикле: вендорные скрипты колонки иногда чистят INPUT.
lock_input() {
    for port in 2222 5555; do
        if [ -n "$PC" ]; then
            iptables -C INPUT -i wlan0 -p tcp --dport "$port" -s "$PC" -j ACCEPT 2>/dev/null \
              || iptables -I INPUT 1 -i wlan0 -p tcp --dport "$port" -s "$PC" -j ACCEPT
        fi
        iptables -C INPUT -i wlan0 -p tcp --dport "$port" -j DROP 2>/dev/null \
          || iptables -A INPUT -i wlan0 -p tcp --dport "$port" -j DROP
    done
    iptables -C INPUT -i wlan0 -p tcp --dport 1961 -j DROP 2>/dev/null \
      || iptables -A INPUT -i wlan0 -p tcp --dport 1961 -j DROP
    iptables -C INPUT -i wlan0 -p udp --dport 5600 -j DROP 2>/dev/null \
      || iptables -A INPUT -i wlan0 -p udp --dport 5600 -j DROP
}

case "${1:-once}" in
  start)
    lock_input
    # фоновый цикл: 3 неудачи подряд (30 с) -> lite; 2 успеха подряд (20 с) -> pc
    start-stop-daemon -K -q -p "$RUN/failover.pid" 2>/dev/null; rm -f "$RUN/failover.pid"
    start-stop-daemon -S -b -m -p "$RUN/failover.pid" -x /bin/sh -- "$0" loop
    ;;
  stop)
    start-stop-daemon -K -q -p "$RUN/failover.pid" 2>/dev/null; rm -f "$RUN/failover.pid"
    ;;
  loop)
    fails=0; oks=0
    while :; do
        lock_input
        if pc_alive; then oks=$((oks+1)); fails=0; else fails=$((fails+1)); oks=0; fi
        m=$(cur_mode)
        # без флага use_pc ПК не ждём: в lite сразу после загрузки, а не через 30 с
        if [ ! -f "$CFG/use_pc" ] && [ "$m" != lite ]; then to_lite; fails=0; m=lite; fi
        if [ "$fails" -ge 3 ] && [ "$m" != lite ]; then to_lite; fails=0; fi
        # lite упал — поднимаем: в автономном режиме кроме него отвечать некому
        if [ "$m" = lite ]; then ensure_lite; ensure_mic_nat; fi
        if [ "$oks" -ge 2 ] && [ "$m" = lite ]; then to_pc; oks=0; fi
        sleep 10
    done
    ;;
  once)
    if pc_alive; then to_pc; else to_lite; fi
    ;;
  status)
    echo "mode=$(cur_mode) pc_alive=$(pc_alive && echo yes || echo no)"
    iptables -t nat -L OUTPUT -n 2>/dev/null | grep -c REDIRECT
    ;;
  *) echo "usage: failover.sh start|stop|loop|once|status"; exit 1 ;;
esac
