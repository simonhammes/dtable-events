# Branch improve-pdf-generation

## Goals
- Decouple PDF generation from the main SeaTable system
- Allow resource limits since a headless Chrome instance uses quite a lot of resources
  - Currently, SeaTable and Chrome share the resources inside the container, potentially causing performance problems in SeaTable itself
- Allow for variable scalability
  - This approach allows you to run 1 or 10+ Gotenberg instances depending on your needs

## Required Changes

### Run Gotenberg Instances

Docs: [gotenberg.dev](https://gotenberg.dev/)

Update `seatable-server.yml`:

```yml
services:
  gotenberg-1:
    image: gotenberg/gotenberg:8.5.0
    networks:
      - backend-seatable-net
  gotenberg-2:
    image: gotenberg/gotenberg:8.5.0
    networks:
      - backend-seatable-net
  gotenberg-3:
    image: gotenberg/gotenberg:8.5.0
    networks:
      - backend-seatable-net
  gotenberg-4:
    image: gotenberg/gotenberg:8.5.0
    networks:
      - backend-seatable-net
  gotenberg-5:
    image: gotenberg/gotenberg:8.5.0
    networks:
      - backend-seatable-net
```

*Note:* Settings `mode.replicas` (https://docs.docker.com/compose/compose-file/deploy/#replicas) to 5 is easier,
but gives you less control over the load balacing algorithm.

### Update nginx.conf

```
upstream gotenberg {
    server gotenberg-1:3000;
    server gotenberg-2:3000;
    server gotenberg-3:3000;
    server gotenberg-4:3000;
    server gotenberg-5:3000;
}

server {
    # ...

    # Proxy /gotenberg/* to Gotenberg instances and strip /gotenberg prefix
    location /gotenberg/ {
        # Trailing slash is important
        proxy_pass http://gotenberg/;
    }
}
```

### Update gunicorn.py

The default Gunicorn configuration (5 workers with 5 threads each) is not enough to test mass PDF generation
(e.g. generating 30 PDF files using a _Button_ column). Otherwise dtable-web will hang since the HTTP connections
from the browser to `/api/v2.1/workspace/{}/dtable/{}/page-design-file/` are kept open **until** the PDF file
is generated and uploaded.

```py
workers = 5
threads = 20
```

### Code Changes

The required code changes in `dtable-events` are on this branch.

## Architectural Improvements

TODO
