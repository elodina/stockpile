# go build github.com/elodina/stockpile
go build cli.go
./cli scheduler --master 10.1.13.244:5050 --api http://10.1.13.244:6666 --log.level=debug
