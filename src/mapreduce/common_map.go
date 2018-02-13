package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
)

func checkError(e error) {
	if e != nil {
		panic(e)
	}
}

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	mapData, err := ioutil.ReadFile(inFile)
	checkError(err)

	pairs := mapF(inFile, string(mapData))

	reduceData := make([][]KeyValue, nReduce)

	for i := 0; i < nReduce; i++ {
		reduceData[i] = make([]KeyValue, 0)
	}

	for _, pair := range pairs {
		tag := ihash(pair.Key) % nReduce
		reduceData[tag] = append(reduceData[tag], pair)
	}

	for i := 0; i < nReduce; i++ {
		jsonData, err := json.Marshal(reduceData[i])
		checkError(err)
		fileName := reduceName(jobName, mapTask, i)
		ioutil.WriteFile(fileName, jsonData, 0644)
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
