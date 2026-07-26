sudo docker compose up --build --scale empty=4 -d \
&& sleep 60 \
&& sudo docker compose logs empty > logs.txt \
; sudo docker compose down \
&& grep "^empty-1" logs.txt > logs-1.txt \
&& grep "^empty-2" logs.txt > logs-2.txt \
&& grep "^empty-3" logs.txt > logs-3.txt \
&& grep "^empty-4" logs.txt > logs-4.txt


# sudo docker compose up --build --scale empty=1 -d \
# && sleep 2 \
# && sudo docker compose up --scale empty=2 -d \
# && sleep 2 \
# && sudo docker compose logs empty > logs-3.txt \
# && grep "^empty-2" logs-3.txt > logs-2.txt \
# && sudo docker compose up --scale empty=1 -d \
# && sleep 2 \
# && sudo docker compose logs empty > logs-3.txt \
# && grep "^empty-1" logs-3.txt > logs-1.txt \
# ; sudo docker compose down
