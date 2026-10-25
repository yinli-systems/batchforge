"""Fixed loopback ingress choices for the finite Agent qualification only."""
ENDPOINTS=('http://127.0.0.1:8080','http://127.0.0.1:18172')


def qualification_endpoint(value):
    if value not in ENDPOINTS:
        raise ValueError('Qualification requires an existing primary or private schema canary ingress')
    return value
