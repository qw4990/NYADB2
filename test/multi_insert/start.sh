## create a table
go run ../../client/launcher.go < create.input

## start 40 threads to insert forever
## these 40 threads will not be committed
for ((i = 0; i < 40; i++))
do
	echo "Start backend transaction."
	go run ../../client/launcher.go < input$i.input > /dev/null &
done

## start 5 threads to insert
## these 5 threads will be committed
for ((i = 0; i < 5; i++))
do
	echo "Transaction: " $i
	go run ../../client/launcher.go < Cinput$i.input
done

echo "Done"
