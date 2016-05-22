go run ../../client/launcher.go < create.input

for ((i = 1; i < 40; i++))
do
	echo "Start backend transaction."
	go run ../../client/launcher.go < input$i.input > /dev/null &
done

for ((i = 0; i < 5; i++))
do
	echo "Transaction: " $i
	go run ../../client/launcher.go < Cinput$i.input
done

echo "Done"
