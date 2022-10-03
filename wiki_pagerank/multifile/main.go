package main

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
)

type MediaWiki struct {
	XMLName xml.Name `xml:"mediawiki"`
	Page    []Page   `xml:"page"`
}

type Page struct {
	XMLName xml.Name `xml:"page"`
	Title   string   `xml:"title"`
	Text    string   `xml:"revision>text"`
}

type Combine struct {
	name  string
	score float64
}

type PageRankProcessor struct {
	baseXml    string
	page       *MediaWiki
	rootDir    string
	indexFiles []string
	pageFiles  []string
	nameCnt    int
	nameMap    map[string]int
	// concurrent need fix
	//edge map[int][]int
	edge     [][]int
	edgeCnt  []int
	iteTime  int
	prAlpha  float64
	fin      []Combine
	curScore []float64
	parallel int
}

func (t *PageRankProcessor) Config(rootDir string, iteTime int, prAlpha float64, parallel int) {
	t.baseXml = "{http://www.mediawiki.org/xml/export-0.10/}"
	t.rootDir = rootDir
	t.nameMap = map[string]int{}
	//t.edge = map[int][]int{}
	t.iteTime = iteTime
	t.prAlpha = prAlpha
	t.parallel = parallel
}

func rawXmlToStr(raw string) (str string) {
	str = raw
	str = strings.ReplaceAll(str, "&amp;", "&")
	str = strings.ReplaceAll(str, "&lt;", "<")
	str = strings.ReplaceAll(str, "&gt;", ">")
	str = strings.ReplaceAll(str, "&quot;", "\"")
	str = strings.ReplaceAll(str, "&apos;", "'")
	return
}

func (t *PageRankProcessor) indexParser(fileName string) error {
	path := t.rootDir + fileName
	log.Printf("loading index %+v", path)
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("openfile fail")
	}
	defer func() { _ = file.Close() }()
	buf := bufio.NewReader(file)
	for {
		line, err := buf.ReadString('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("readfile err")
		}
		if err == io.EOF {
			return nil
		}
		start := strings.Index(line, ":") + 1
		start = start + strings.Index(line[start:], ":")
		name := line[start+1 : len(line)-1]
		name = rawXmlToStr(name)
		t.nameMap[name] = t.nameCnt
		t.nameCnt += 1
	}
}

func (t *PageRankProcessor) pageParser(fileName string, pool chan int) error {
	path := t.rootDir + fileName
	log.Printf("loading page %+v", path)
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("openfile fail")
	}
	defer func() { _ = file.Close() }()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("page read all err")
	}
	mediaWiki := &MediaWiki{}
	err = xml.Unmarshal(data, &mediaWiki)
	if err != nil {
		fmt.Printf("err in unmarshal\n")
		return err
	}
	// release mem
	data = []byte{}
	log.Printf("success parse xml page %+v", path)
	regex := "\\[\\[.*?\\]\\]"
	pattern, err := regexp.Compile(regex)
	var name string
	defer func() {
		if r := recover(); r != nil {
			log.Panicln(name)
		}
	}()
	// cur is stable, nex will fetch other pages
	for _, page := range mediaWiki.Page {
		title := page.Title
		cur, ok := t.nameMap[title]
		if !ok {
			log.Panicf("nameMap not hit %+v", title)
		}
		matches := pattern.FindAllString(page.Text, -1)
		for _, str := range matches {
			if index := strings.Index(str, "|"); index > 0 {
				name = str[2:index]
			} else {
				name = str[2 : len(str)-2]
			}
			if len(name) == 0 {
				continue
			}
			if name[0] >= 'a' && name[0] <= 'z' {
				tmp := []rune(name)
				tmp[0] = tmp[0] - 'a' + 'A'
				name = string(tmp)
			}
			nex, ok := t.nameMap[name]
			if !ok {
				continue
			}
			t.edgeCnt[cur] += 1
			t.edge[cur] = append(t.edge[cur], nex)
			//			t.edge[nex] = append(t.edge[nex], cur)
		}
		//		fmt.Printf("%d\n", t.edgeCnt[cur])
	}
	log.Printf("success loading page %+v", path)
	return nil
}

func (t *PageRankProcessor) prepareFiles() {
	files, err := ioutil.ReadDir(t.rootDir)
	if err != nil {
		log.Panicln("readDir fail")
		return
	}
	for _, file := range files {
		name := file.Name()
		if strings.Contains(name, "index") {
			t.indexFiles = append(t.indexFiles, name)
		} else if strings.Contains(name, "xml") {
			t.pageFiles = append(t.pageFiles, name)
		}
	}
	log.Println("into indexParser")
	for _, name := range t.indexFiles {
		if err := t.indexParser(name); err != nil {
			log.Panicf("%+v\n", err)
		}
	}
	log.Println("pass indexParser, nameCnt is ", t.nameCnt)
	t.edgeCnt = make([]int, t.nameCnt)
	t.edge = make([][]int, t.nameCnt)

	log.Println("into pageParser")
	wg := &sync.WaitGroup{}
	wg.Add(len(t.pageFiles))
	pool := make(chan int, t.parallel)
	for i := 0; i < t.parallel; i += 1 {
		pool <- 1
	}
	var wgErr error
	for index := range t.pageFiles {
		name := t.pageFiles[index]
		go func() {
			defer func() {
				//release pool
				pool <- 1
				wg.Done()
			}()
			select {
			case <-pool:
				if err := t.pageParser(name, pool); err != nil {
					wgErr = err
					log.Panicf("%+v\n", err)
				}
			}
		}()
	}
	wg.Wait()
	if wgErr != nil {
		log.Panicln("err in sync group")
	}
}

func (t *PageRankProcessor) iteCalc() {
	log.Println("into iteCalc")
	t.curScore = make([]float64, t.nameCnt)
	for index := range t.curScore {
		t.curScore[index] = 1.0 / float64(t.nameCnt)
	}
	for time := 0; time < t.iteTime; time += 1 {
		t.calcPageRank()
	}
}

func (t *PageRankProcessor) calcPageRank() {
	oldScore := t.curScore
	t.curScore = make([]float64, t.nameCnt)
	var pool float64
	pool = 0.0
	for i := 0; i < t.nameCnt; i += 1 {
		cnt := t.edgeCnt[i]
		// 无出边，吸收
		if cnt == 0 {
			pool += oldScore[i]
			continue
		}
		allNex := t.edge[i]
		for _, nex := range allNex {
			t.curScore[nex] += oldScore[i] / float64(cnt)
		}
	}
	for i := 0; i < t.nameCnt; i += 1 {
		t.curScore[i] *= t.prAlpha
		t.curScore[i] += pool*t.prAlpha/float64(t.nameCnt) + (1.0-t.prAlpha)/float64(t.nameCnt)
	}

}

func (t *PageRankProcessor) collect() {
	// realise mem
	t.edge = make([][]int, 0)
	t.edgeCnt = make([]int, 0)
	log.Println("collecting pagerank")
	for name, index := range t.nameMap {
		//		fmt.Printf("%+v\n", t.curScore[index])
		t.fin = append(t.fin, Combine{name, t.curScore[index]})
	}
	sort.Slice(t.fin, func(i, j int) bool {
		return t.fin[i].score > t.fin[j].score
	})
}

func (t *PageRankProcessor) Run() []Combine {
	t.prepareFiles()
	t.iteCalc()
	t.collect()
	return t.fin
}

func main() {
	processor := &PageRankProcessor{}
	processor.Config("../../../data/", 50, 0.9, 2)
	filePath := processor.rootDir + "pagerank.txt"
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Panicf("open file err, err is %+v", err)
		return
	}
	defer func() { _ = file.Close() }()

	for _, item := range processor.Run() {
		line := fmt.Sprintf("%s\t%.6f\n", item.name, item.score)
		//		println(line)
		_, err := file.WriteString(line)
		if err != nil {
			log.Panicf("write file err, err is %+v", err)
		}
	}
}
