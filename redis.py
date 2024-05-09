import redis

# # 连接到本地 Redis 服务
r = redis.Redis(host='localhost', port=6379, db=0)

# # 设置一个键
# r.set('test_key', 'hello, Redis!')

# # 获取键的值
# value = r.get('test_key')
# print(value)  # 输出：b'hello, Redis!'

# # 使用键值对
# r.hset("hash1", "key1", "value1")
# r.hset("hash1", "key2", "value2")

# # 获取哈希表的值
# value1 = r.hget("hash1", "key1")
# print(value1)  # 输出：b'value1'

# # 删除键
r.delete('conversation_history')
