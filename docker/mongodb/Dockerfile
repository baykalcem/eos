FROM mongo:noble

COPY mongodb/generate_keyfile.sh /root/generate_keyfile.sh
RUN /bin/bash /root/generate_keyfile.sh

COPY mongodb/mongod.conf /etc/mongod.conf
COPY mongodb/init_mongodb.js /docker-entrypoint-initdb.d/init_mongodb.js

CMD ["mongod", "-f", "/etc/mongod.conf"]
