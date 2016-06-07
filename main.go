package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

var tempDir, _ = ioutil.TempDir("", "pdoop")
var parallel = flag.Int("p", 10, "parallel")
var mergeMode = flag.Bool("m", false, "merge as a single file")

type HDFS struct {
	hadoopHome string
	hadoopCmd  string
}

func NewHDFS() HDFS {
	hadoopHome := os.Getenv("HADOOP_HOME")
	return HDFS{
		hadoopHome: hadoopHome,
		hadoopCmd:  path.Join(hadoopHome, "bin", "hadoop"),
	}
}

func (s HDFS) Ls(inputPath string) []string {
	log.Printf("ls: %s\n", inputPath)
	output := s.Exec("fs", "-ls", inputPath)
	outputSplit := strings.Split(output, "\n")
	files := []string{}
	for _, line := range outputSplit {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "-") || line == "" {
			continue
		}
		split := strings.Fields(line)
		files = append(files, split[len(split)-1])
	}
	return files
}

func (s HDFS) Get(remote string, local string) {
	s.Exec("fs", "-get", remote, local)
}

func (s HDFS) Gets(remoteFiles []string, local string) {
	for _, f := range remoteFiles {
		s.Get(f, local)
	}
}

func (s HDFS) Exec(subCmd string, args ...string) string {
	jobConf := []string{}
	fullCmd := []string{
		subCmd,
	}
	for _, args := range [][]string{jobConf, args} {
		fullCmd = append(fullCmd, args...)
	}
	cmd := exec.Command(s.hadoopCmd, fullCmd...)
	stdout, _ := cmd.CombinedOutput()
	return string(stdout)
}

func downloadWithChan(hdfs HDFS, remoteFiles []string, localDir string, ch chan string) {
	for _, f := range remoteFiles {
		hdfs.Get(f, localDir)
		localBase := filepath.Base(f)
		ch <- path.Join(localDir, localBase)
	}
}

func chunk(lst []string, chunkNum int) (output [][]string) {
	chunkSize := int(math.Ceil(float64(len(lst)) / float64(chunkNum)))
	for i := 0; i < chunkNum; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(lst) {
			break
		}
		if end > len(lst) {
			end = len(lst)
		}
		output = append(output, lst[start:end])
	}
	return output
}

func checkDir(path string) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("path is not a directory: %s", path)
	}
	return nil
}

func main() {
	flag.Parse()
	if flag.NArg() != 2 {
		fmt.Println("Two arguments (input and output) are required!")
		flag.Usage()
		os.Exit(1)
	}
	input := flag.Arg(0)
	output := flag.Arg(1)
	log.Println("input: ", input)
	log.Println("output: ", output)
	if !*mergeMode {
		err := checkDir(output)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
	}
	hdfs := NewHDFS()
	files := hdfs.Ls(input)
	slices := chunk(files, *parallel)
	ch := make(chan string, len(files))
	var wg sync.WaitGroup
	if *mergeMode {
		log.Println("tempDir: ", tempDir)
		defer os.RemoveAll(tempDir)
		for _, files := range slices {
			wg.Add(1)
			go func(files []string) {
				defer wg.Done()
				downloadWithChan(hdfs, files, tempDir, ch)
			}(files)
		}
	} else {
		for _, files := range slices {
			wg.Add(1)
			go func(files []string) {
				defer wg.Done()
				hdfs.Gets(files, output)
			}(files)
		}

	}
	if *mergeMode {
		go func() {
			wg.Wait()
			close(ch)
		}()
		out, _ := os.Create(output)
		for f := range ch {
			in, _ := os.Open(f)
			if _, err := io.Copy(out, in); err != nil {
				log.Fatal(err)
			}
			in.Close()
			os.Remove(f)
		}
		out.Close()
	} else {
		wg.Wait()
		close(ch)
	}
}
