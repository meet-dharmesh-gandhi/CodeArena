FROM node:20-bookworm

RUN apt-get update && apt-get install -y build-essential python3 && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY ./src/*.c ./src/

COPY ./include/all.h ./include/async.h ./include/async.c ./include/constants.h ./include/memory.h ./include/memory.c ./include/morph.h ./include/morph.c ./include/network.h ./include/network.c ./include/print.h ./include/print.c ./include/utils.h ./include/utils.c ./include/

COPY compile-in-docker.sh ./

COPY ./src/node/package*.json ./src/node/binding.gyp ./src/node/gateway.* ./src/node/

RUN cd ./src/node && npm install && cd ..

RUN chmod 777 compile-in-docker.sh

RUN bash ./compile-in-docker.sh

EXPOSE 8000 8001 8002 8003 8004 8005

CMD ["./src/empty"]
