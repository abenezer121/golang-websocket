scalable websocket connection 


i tested 500000 socket connection distributed from different docker instance 
ps aux | grep "go run server.go" | grep -v "grep" | awk '{print $6/1024 " MB"}'
18.4648 MB


