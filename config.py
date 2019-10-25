__all__ = ['local_env']

with open('.env', 'r') as f:
    local_env = {
        key.strip(): value.strip()
        for (key, value) in [
            line.split('=') for line
            in f.read().split('\n') if '=' in line
        ]
    }
