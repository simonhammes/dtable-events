# Branch improve-pdf-generation

## Goals
- Decouple PDF generation from the main SeaTable system
- Allow resource limits since a headless Chrome instance uses quite a lot of resources
  - Currently, SeaTable and Chrome share the resources inside the container, potentially causing performance problems in SeaTable itself
- Allow for variable scalability

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
but gives you less control over the load balancing algorithm.
Instead, I've decided to use NGINX as a reverse proxy in front of the Gotenberg instances. NGINX employs a round-robin
load balancing algorithm by default, which seemed to perform better since it prevents sequential PDF generation requests to hit the
same Gotenberg instance (causing the second request to take much longer).

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

### Increase Number of dtable-events Workers

Increase the number of workers (default: 3) in `dtable-events.conf`. Ideally, this number should match (or exceed) the number of Gotenberg instances.

```ini
[DTABLE-IO]
workers = 5
```

### Code Changes

The required code changes in `dtable-events` are on this branch.

## ToDos

Since this is a POC, there are some remaining issues/tasks:
- Gotenberg uses a newer version of Chrome (124 vs 96 in the SeaTable container) which apparently causes layout changes
  - This causes an extra blank page at the end of each document
  - The problem also occurs if you open a generated URL (e.g. `https://DOMAIN/dtable/d5910317-0477-4b32-800e-1c1a9b5b441f/page-design/ZKyC/row/F_tdujaNTXu98fbssjF2eQ/?access-token=TOKEN&need_convert=0`) in your browser and try to print it
  - This is probably caused by the `<div data-evergreen-toaster-container>` element at the end of the body
  - If you manually remove this element from the DOM, the extra blank page is gone
- Sometimes (this seems to happen quite rarely) images from the page design template are missing :/
  - This seems surprising since Gotenberg waits for the network to be idle by default (https://gotenberg.dev/docs/routes#performance-mode-chromium)
  - Unfortunately I haven't found a way to reproduce this
- Do more thorough testing and compare generated PDFs (especially for more complex page design templates)
- Do not allow public access to Gotenberg instances
- Evaluate Gotenberg usage for all PDF-related operations (e.g. automation rules)

## Architectural Improvements

- Remove coupling between dtable-events and dtable-web
  - dtable-events stores the generated PDF file in `/tmp`, which `dtable-web` then directly accesses over the filesystem
  - Storing the generated file in a remote location (Seafile/S3/...) or transferring it over the network would remove this coupling
- Try to use the existing WS connection between the client and SeaTable instead of polling (dtable-web => dtable-events to check if the task has finished) and keeping HTTP connections open (Browser => dtable-web) for a more efficient transport mechanism
- Use a proper queue (e.g. Redis) to run a variable number of workers (each in their own `process`)
  - Right now they run inside the same process
