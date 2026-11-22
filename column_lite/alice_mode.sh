#!/bin/sh
# Переключатель режимов: oleg (свой бэкенд на ПК) / alisa (родное облако Яндекса).
# Живёт на колонке в /data/opt/bin/alice_mode.sh; его дёргает /etc/init.d/S97alicewatch.
Q=/system/vendor/quasar/quasar.cfg
U=/etc/unbound/unbound.conf
C=/data/opt/cfg
M=/data/opt/mode
cur() {
  if [ -f "$M" ]; then cat "$M"; else echo oleg; fi
}
case "$1" in
  status) cur; exit 0 ;;
  oleg|alisa) ;;
  *) echo "usage: alice_mode.sh [oleg|alisa|status]"; exit 1 ;;
esac
# АВТОНОМНЫЙ РЕЖИМ (решение 17.11): колонка работает только на своём lite, сервер
# на ПК её больше не обслуживает. S97alicewatch при живом ПК мог попросить «oleg»
# и увести колонку на ПК — без флага отладки такой запрос игнорируем.
# Вернуть старое поведение: touch /data/opt/cfg/use_pc
if [ "$1" = oleg ] && [ ! -f "$C/use_pc" ]; then
  echo "автономный режим: переключение на ПК отключено (нет $C/use_pc)"
  exit 0
fi
if [ "$(cur)" = "$1" ]; then echo "уже $1"; exit 0; fi
mount -o remount,rw / 2>/dev/null
cp "$C/quasar.$1" "$Q" && cp "$C/unbound.$1" "$U" && echo "$1" > "$M" && sync
/etc/init.d/S70unbound restart >/dev/null 2>&1
/etc/init.d/S99quasar restart >/dev/null 2>&1
mount -o remount,ro / 2>/dev/null
echo "переключено на $1"
