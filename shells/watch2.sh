while true; do
  clear
  echo "========== Redis 监控 =========="
  echo "当前时间: $(date '+%H:%M:%S')"
  echo ""
  echo "1. 拥有条件单的活跃客户数:"
  docker exec deploy-redis-1 redis-cli SCARD qsys:active_clients
  echo ""
  echo "2. Canal 最新同步位点 (Binlog pos):"
  docker exec deploy-redis-1 redis-cli GET qsys:canal:pos
  sleep 2
done