while true; do
  clear
  echo "========== MySQL 监控 =========="
  echo "当前时间: $(date '+%H:%M:%S')"
  echo ""
  docker exec deploy-mysql-1 mysql -uroot -proot conorder_db -e "
    SELECT '总订单量' AS metrics, COUNT(*) AS value FROM orders;
    SELECT '' AS '', '' AS '';
    SELECT '最新落库的 10 条订单' AS status;
    SELECT id, client_id, exchange_type, stock_code FROM orders ORDER BY id DESC LIMIT 10;"
  sleep 2
done