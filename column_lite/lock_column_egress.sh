#!/bin/sh
# /data/quasar/lock_column_egress.sh — колонке можно в локалку и в медиа-CDN,
# но НЕ в Яндекс.
#
# Зачем: колонка должна (а) не отправлять метрики/не ходить в облако Алисы,
# (б) не получить OTA/brick от Яндекса, (в) уметь сама качать музыку, аудиокниги
# и радио по прямым URL, когда ПК выключен.
#
# Предыдущая версия блокировала ВСЁ, кроме LAN, и добавляла разрешения для CDN
# ПОСЛЕ терминального DROP — они были недостижимы (счётчики 0), поэтому музыка,
# книги и радио не играли никогда. Здесь порядок правильный: сначала блок
# Яндекса по подсетям (адреса, а не DNS — DNS-пин может слететь), затем
# разрешение на медиа, и только в конце DROP для всего остального.
#
# Вызывается из onboot.sh ДО run.sh. Адреса сети в репозитории не зашиты:
# LAN и роутер определяются по wlan0/default route (или задаются файлами
# $CFG/lan, $CFG/router_ip / переменными LAN_NET, ROUTER_IP). IP компьютера
# quasard — $CFG/pc_ip (пишет deploy_lite.sh --pc-ip) или PC_IP.

CFG=/data/opt/cfg
LAN="${LAN_NET:-$(cat "$CFG/lan" 2>/dev/null || true)}"
ROUTER="${ROUTER_IP:-$(cat "$CFG/router_ip" 2>/dev/null || true)}"
PC="${PC_IP:-$(cat "$CFG/pc_ip" 2>/dev/null || true)}"
case "$PC" in *[!0-9.]*) PC="";; esac
SELF_IP=$(ip -4 addr show wlan0 2>/dev/null \
    | grep -o 'inet [0-9.]*' | head -1 | cut -d' ' -f2)
[ -n "$LAN" ] || { [ -n "${SELF_IP:-}" ] && LAN="${SELF_IP%.*}.0/24"; }
[ -n "$ROUTER" ] || ROUTER=$(ip route 2>/dev/null | awk '/^default/ {print $3; exit}')

# Подсети Яндекса (IPv4). Список покрывает облако Алисы, quasar-backend,
# метрику/appmetrica, паспорт, OTA-серверы. Проверено резолвом 15.11.2026:
# uniproxy 213.180.193.76, quasar 213.180.193.230, yandex.ru 5.255.255.77,
# passport 93.158.134.39, radio 213.180.204.186, avatars 87.250.247.181.
YANDEX_NETS="
5.255.255.0/24
37.9.64.0/18
77.88.0.0/18
87.250.224.0/19
93.158.128.0/18
95.108.128.0/17
100.64.0.0/10
130.81.0.0/16
141.8.128.0/18
178.154.128.0/17
185.32.184.0/22
199.21.86.0/24
213.180.192.0/19
217.20.144.0/20
"

for IPT in iptables ip6tables; do
    "$IPT" -N column_local_only 2>/dev/null
    "$IPT" -F column_local_only 2>/dev/null || continue
    "$IPT" -D OUTPUT -j column_local_only 2>/dev/null
    "$IPT" -I OUTPUT 1 -j column_local_only

    # 1) своё и уже установленные соединения
    "$IPT" -A column_local_only -o lo -j RETURN
    "$IPT" -A column_local_only -m state --state ESTABLISHED,RELATED -j RETURN

    if [ "$IPT" = "iptables" ]; then
        # 1б) АВТОНОМНЫЙ РЕЖИМ: к серверу на ПК колонка сама не ходит (веб 80/443
        # и поток микрофона 5600), даже если ПК включён. В lite-режиме эти адреса
        # заворачиваются NAT-ом на саму колонку ДО этой цепочки и сюда не попадают;
        # правило ловит только окно после загрузки, пока NAT ещё не поставлен.
        # Отладка с ПК по ADB/SSH не страдает: это входящие соединения
        # (ответы проходят как ESTABLISHED). Вернуть ПК: touch /data/opt/cfg/use_pc
        if [ ! -f /data/opt/cfg/use_pc ] && [ -n "$PC" ]; then
            "$IPT" -A column_local_only -d "$PC" -p tcp --dport 80 -j DROP
            "$IPT" -A column_local_only -d "$PC" -p tcp --dport 443 -j DROP
            "$IPT" -A column_local_only -d "$PC" -p udp --dport 5600 -j DROP
        fi
        # 2) LAN: телефон, управление, отладка
        [ -n "$LAN" ] && "$IPT" -A column_local_only -d "$LAN" -j RETURN
        # 3) DHCP и DNS на роутер (unbound колонки форвардит туда)
        "$IPT" -A column_local_only -d 255.255.255.255 -p udp --dport 67:68 -j RETURN
        [ -n "$ROUTER" ] && "$IPT" -A column_local_only -d "$ROUTER" -p udp --dport 53 -j RETURN
        # 3б) DNS через 77.88.8.x: unbound.conf форвардит ИМЕННО туда, а это
        # подсеть 77.88.0.0/18, которая ниже в DROP — без этого разрешения
        # пропадала резолюция (Temporary failure in name resolution) и музыка
        # в автономном режиме не находилась. DNS — не метрики, пускаем.
        "$IPT" -A column_local_only -d 77.88.8.8 -p udp --dport 53 -j RETURN
        "$IPT" -A column_local_only -d 77.88.8.1 -p udp --dport 53 -j RETURN
        # 3в) настоящий uniproxy (голос в relay-режиме lite): конкретные IP,
        # а не подсеть. Раньше ручное REJECT netguard мешал — здесь явный RETURN
        # ВЫШЕ DROP подсетей 213.180.192.0/19.
        "$IPT" -A column_local_only -d 213.180.193.76/32 -p tcp --dport 443 -j RETURN
        "$IPT" -A column_local_only -d 213.180.204.80/32 -p tcp --dport 443 -j RETURN

        # 4) ЯНДЕКС — блок ДО разрешений на медиа.
        for net in $YANDEX_NETS; do
            "$IPT" -A column_local_only -d "$net" -j DROP
        done
        # DNS Яндекса (77.88.8.x) закрыт подсетью 77.88.0.0/18; unbound уйдёт
        # на роутер — это нормальный путь и он разрешён выше.

        # 5) медиа: HTTPS/HTTP наружу (SkySound CDN, archive.org, icecast,
        #    Deezer/iTunes превью, SomaFM, ccmixter).
        "$IPT" -A column_local_only -p tcp --dport 443 -j RETURN
        "$IPT" -A column_local_only -p tcp --dport 80 -j RETURN
        # ICY-радио и часть CDN живут на нестандартных портах: 8443 — «Энергия»
        # (101.ru), раньше она молча умирала по таймауту (проверено 18.11).
        "$IPT" -A column_local_only -p tcp --dport 8000:8500 -j RETURN
    fi

    # 6) всё остальное — в никуда
    "$IPT" -A column_local_only -j DROP
done

echo "egress-lock: активен (LAN=${LAN:-авто}, Яндекс заблокирован, медиа разрешено)"
