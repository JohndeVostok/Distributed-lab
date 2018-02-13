package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	pairs := make(map[string][]string)
	mapData := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		data, err := ioutil.ReadFile(fileName)
		checkError(err)
		err = json.Unmarshal(data, &mapData)
		checkError(err)
		for _, pair := range mapData {
			_, existed := pairs[pair.Key]
			if !existed {
				pairs[pair.Key] = make([]string, 0)
			}
			pairs[pair.Key] = append(pairs[pair.Key], pair.Value)
		}

	}

	keys := make([]string, 0)
	for key, _ := range pairs {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	fileName := mergeName(jobName, reduceTask)
	mergeFile, err := os.Create(fileName)
	checkError(err)
	defer mergeFile.Close()

	encoder := json.NewEncoder(mergeFile)
	for _, key := range keys {
		res := reduceF(key, pairs[key])
		err := encoder.Encode(&KeyValue{key, res})
		checkError(err)
	}
}
