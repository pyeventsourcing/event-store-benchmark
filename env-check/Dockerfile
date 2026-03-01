FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    fio \
    jq \
    util-linux \
    coreutils \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /envcheck

COPY check_env.sh /envcheck/check_env.sh
RUN chmod +x /envcheck/check_env.sh

CMD ["/envcheck/check_env.sh"]