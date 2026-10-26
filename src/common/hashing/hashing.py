import hashlib

def shard_for(num_shards: int, *key_parts) -> int:
    key = ":".join(str(part) for part in key_parts)
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest, 16) % num_shards