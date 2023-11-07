package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"plugin"
	"sort"

	"6.824/mr"
)

// for sorting by key.
// 定义一个类型为KeyValue的数组，类型名为ByKey
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	//如果命令行给定的参数小于三个，则打印错误信息
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map
	// accumulate the intermediate Map output.
	//
	//intermediate 是一个空的 mr.KeyValue 数组
	intermediate := []mr.KeyValue{}
	//从文件夹里读取数据
	for _, filename := range os.Args[2:] {
		//打开文件夹，如果打开成功返回的是文件指针
		file, err := os.Open(filename)
		//如果打开错误，打印日志
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		//通过ioutil.ReadAll函数读取file中的内容，返回值是一个byte数组
		content, err := ioutil.ReadAll(file)
		//如果打开错误，打印打开错误的日志
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		//mapf需要传入两个参数，一个是filename，另一个是文件的内容
		//返回的是mr.KeyValue 类型的数据，分别是单词和对应的次数组成的结构体
		kva := mapf(filename, string(content))
		//fmt.Print(kva)
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//
	//先将[]mr.KeyValue专成ByKey类型的数据，然后定义所需要的三个接口，即能够进行排序
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// fmt.Println(intermediate[i].Key, output)
		// this is the correct format for each line of Reduce output.
		// 将reduce中的内容输出到ofile中
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
// 函数一共有两个返回值，是两个函数，一个是mapf，接受两个string参数，返回一个mr.KeyValue类型的数组
// 另一个返回值是reducef，接受两个参数，返回的是一个string
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
