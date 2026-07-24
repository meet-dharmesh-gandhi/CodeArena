FROM node:20-bookworm

RUN apt-get update && apt-get install -y build-essential python3 && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY ./my-src/*.c ./my-src/

COPY ./include/all.h ./include/async.h ./include/async.c ./include/constants.h ./include/memory.h ./include/memory.c ./include/morph.h ./include/morph.c ./include/network.h ./include/network.c ./include/print.h ./include/print.c ./include/utils.h ./include/utils.c ./include/

COPY compile-in-docker.sh ./

COPY ./my-src/node/package*.json ./my-src/node/binding.gyp ./my-src/node/gateway.* ./my-src/node/

RUN cd ./my-src/node && npm install && cd ..

RUN chmod 777 compile-in-docker.sh

RUN bash ./compile-in-docker.sh

EXPOSE 8000 8001 8002 8003 8004 8005

CMD ["./my-src/empty"]
