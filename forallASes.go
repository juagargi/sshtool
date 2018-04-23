package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var (
	defaultTargetsFilename = func() string {
		usr, err := user.Current()
		if err != nil {
			fmt.Println("Error obtaining current user:", err)
			os.Exit(1)
		}
		return filepath.Join(usr.HomeDir, ".scionlabTargetMachines")
	}()
)

type target struct {
	host string
	port uint16
}

func loadMachines(targetsFile string) []target {
	var machines []target
	file, err := os.Open(targetsFile)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(file)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		port := uint16(22)
		if len(scanner.Text()) > 0 && scanner.Text()[0] == '#' {
			continue
		}
		fields := strings.Fields(scanner.Text())
		switch len(fields) {
		case 0:
			continue
		case 2:
			longPort, err := strconv.ParseUint(fields[1], 10, 16)
			if err != nil {
				fmt.Println("Error parsing the targets file at line", lineNumber, ", expecting host port:", err)
			}
			port = uint16(longPort)
			fallthrough
		case 1:
			machines = append(machines, target{host: fields[0], port: port})
		default:
			fmt.Println("Error parsing the targets file at line", lineNumber, ", expected host port but encountered", len(fields), " fields instead:", scanner.Text())
			os.Exit(1)
		}
	}
	err = scanner.Err()
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	return machines
}

func merge(cs ...<-chan string) <-chan string {
	ret := make(chan string)
	var wg sync.WaitGroup
	output := func(c <-chan string) {
		for n := range c {
			ret <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}
	go func() {
		wg.Wait()
		close(ret)
	}()
	return ret
}

func mergeErrors(cs ...<-chan error) <-chan error {
	ret := make(chan error)
	var wg sync.WaitGroup
	output := func(c <-chan error) {
		for n := range c {
			ret <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}
	go func() {
		wg.Wait()
		close(ret)
	}()
	return ret
}

// FileToChannel returns a channel you can read from, with the contents read from the file.
// It will close the channel when EOF.
func FileToChannel(file io.Reader) (chan string, chan error) {
	ch := make(chan string)
	errch := make(chan error)
	outb := make([]byte, 4096)
	go func() {
		defer close(ch)
		defer close(errch)
		for {
			n, err := file.Read(outb)
			if err == io.EOF {
				break
			} else if err != nil {
				errch <- err
				break
			}
			ch <- string(outb[0:n])
		}
	}()
	return ch, errch
}

func ssh(machine *target, sshOptions []string, command string, output chan<- string, errors chan<- error) error {
	sshOptions = append(sshOptions, "LogLevel=QUIET")
	arguments := []string{}
	for _, o := range sshOptions {
		arguments = append(arguments, "-o", o)
	}
	arguments = append(arguments, "-t", "-p", strconv.Itoa(int(machine.port)), "scion@"+machine.host, command)
	cmd := exec.Command("ssh", arguments...)

	// cmd.Stdin=os.Stdin would cause problems with the terminal running this application (2nd instance of ssh and beyond)
	// so we use the default behavior which is to open os.Devnull

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	err = cmd.Start()
	if err != nil {
		return err
	}

	stdoutdata, stdouterr := FileToChannel(stdout)
	stderrdata, stderrerr := FileToChannel(stderr)
	go func() {
		defer close(output)
		for x := range merge(stdoutdata, stderrdata) {
			output <- x
		}
	}()
	go func() {
		defer close(errors)
		for x := range mergeErrors(stdouterr, stderrerr) {
			errors <- x
		}
		err = cmd.Wait()
		if err != nil {
			errors <- err
		}
	}()
	return nil
}

func runScript(machine *target, sshOptions []string, script string, output chan<- string, errors chan<- error) error {
	remoteScript := "__forAll_script.sh"
	cmd := exec.Command("scp", "-P", strconv.Itoa(int(machine.port)), script, "scion@"+machine.host+":/tmp/"+remoteScript)
	err := cmd.Run()
	if err != nil {
		return err
	}
	return ssh(machine, sshOptions, "cd /tmp;chmod +x "+remoteScript+";. ~/.profile;./"+remoteScript+";EX=$?;rm "+remoteScript+";exit $EX", output, errors)
}

func allOfChannelWithTempFile(ch <-chan string, f *os.File) string {
	var ret string
	for s := range ch {
		ret += s
		_, err := f.WriteString(s)
		if err != nil {
			ret += fmt.Sprintf("FORALL ASes: ERROR writting to temp file: %s", err)
		}
	}
	return ret
}

func usage() {
	fmt.Printf(`Usage:
%s {'commands && to be executed' | -f script_file_here_to_run_there.sh} [-o ssh_options] [-t targets_file]

If -t is not specified, the target machines file will be %s
Example of some of the ssh_options you can specify:
-o ConnectTimeout=1 -o ConnectionAttempts=1
etc.
`, os.Args[0], defaultTargetsFilename)
}

func main() {
	var commands []string
	script := ""
	command := ""
	sshOptions := []string{}
	targetsFile := defaultTargetsFilename
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--help" || os.Args[i] == "-h" {
			usage()
			return
		} else if os.Args[i] == "-t" {
			if len(os.Args) < i+2 {
				usage()
				return
			}
			targetsFile = os.Args[i+1]
			i++
		} else if os.Args[i] == "-o" {
			if len(os.Args) < i+2 {
				usage()
				return
			}
			sshOptions = append(sshOptions, os.Args[i+1])
			i++
		} else if os.Args[i] == "-f" {
			if len(os.Args) < i+2 || len(commands) > 0 {
				usage()
				return
			}
			script = os.Args[i+1]
			i++
		} else {
			if script != "" {
				usage()
				return
			}
			commands = append(commands, os.Args[i])
		}
	}
	if script == "" {
		if len(commands) == 0 {
			usage()
			return
		}
		command = strings.Join(commands, ";")
		// amend command:
		command = ". ~/.profile;" + command
	} else if _, err := os.Stat(script); err != nil {
		fmt.Println("Error with script file:", err)
		os.Exit(1)
	}

	machines := loadMachines(targetsFile)

	tempDir, err := ioutil.TempDir("", "__forallASes_temp_")
	if err != nil {
		fmt.Println("Error creating temporary directory:", err)
		os.Exit(10)
	}

	fmt.Printf("Start ssh for %d machines\n", len(machines))
	outputs := make([]chan string, len(machines))
	errors := make([]chan error, len(machines))
	tempFiles := make([]*os.File, len(machines))
	for i := range machines {
		outputs[i] = make(chan string)
		errors[i] = make(chan error)

		tempFile := fmt.Sprintf("channel_%s", machines[i].host)
		tempFile = filepath.Join(tempDir, tempFile)
		f, err := os.Create(tempFile)
		if err != nil {
			fmt.Printf("ERROR: cannot open temp file %s: %v", tempFile, err)
			os.Exit(30)
		}
		defer f.Close()
		tempFiles[i] = f
	}
	setter := func(i int) {
		var err error
		if script == "" {
			err = ssh(&machines[i], sshOptions, command, outputs[i], errors[i])
		} else {
			err = runScript(&machines[i], sshOptions, script, outputs[i], errors[i])
		}
		if err != nil {
			close(outputs[i])
			errors[i] <- err
			close(errors[i])
		}
		fmt.Printf("Started %d / %d\n", i+1, len(machines))
	}
	for i := range machines {
		go setter(i)
	}
	output := make([]string, len(machines))
	sync := make(chan int, len(machines))
	for i := 0; i < len(machines); i++ {
		go func(i int) {
			output[i] = allOfChannelWithTempFile(outputs[i], tempFiles[i])
			sync <- i
		}(i)
	}
	// execution has finished here for all targets
	summarizedOutput := make(map[string][]int) // output to machine index
	for i := 0; i < len(machines); i++ {
		machineIdx := <-sync
		out := output[machineIdx]
		summarizedOutput[out] = append(summarizedOutput[out], machineIdx)
		fmt.Printf("    Done %d / %d        Machine %s \n", i+1, len(machines), machines[machineIdx].host)
	}
	// summarized output:
	outputIndex := 1
	for k, v := range summarizedOutput {
		fmt.Println("-----------------------------------------------")
		fmt.Printf("-- Output %d / %d :\n", outputIndex, len(summarizedOutput))
		fmt.Println("---- BEGIN -----------------------------------")
		fmt.Print(k)
		fmt.Println("---- END --------------------------------------")
		fmt.Println("For targets:")
		for _, i := range v {
			fmt.Printf("%v ", machines[i].host)
		}
		fmt.Println()
		outputIndex++
	}
	fmt.Println("-----------------------------------------------")

	// errors:
	donePrintErrorHeader := false
	printErrorHeader := func() {
		if !donePrintErrorHeader {
			fmt.Println("----------- ERRORS ---------------------------------------------------------")
			donePrintErrorHeader = true
		}
	}
	summarizedOutput = make(map[string][]int)
	for i, ch := range errors {
		output[i] = ""
		for x := range ch {
			output[i] += fmt.Sprintf("%v", x)
		}
	}
	for i, msgs := range output {
		if msgs != "" {
			printErrorHeader()
			// summarize the errors
			summarizedOutput[msgs] = append(summarizedOutput[msgs], i)
		}
	}
	outputIndex = 1
	for k, v := range summarizedOutput {
		fmt.Println("-----------------------------------------------")
		fmt.Printf("-- ERROR %d / %d :\n", outputIndex, len(summarizedOutput))
		fmt.Println("---- BEGIN -----------------------------------")
		fmt.Print(k)
		fmt.Println("\n---- END --------------------------------------")
		fmt.Println("For targets:")
		for _, i := range v {
			fmt.Printf("%v ", machines[i].host)
		}
		fmt.Println()
		outputIndex++
	}
	fmt.Printf("----------------------------------------------------------------------------\n")

	// only now delete the temporary directory
	err = os.RemoveAll(tempDir)
	if err != nil {
		fmt.Printf("Error removing temp directory %s: %v\n", tempDir, err)
		os.Exit(20)
	}
	if donePrintErrorHeader {
		fmt.Println("Finished with errors")
		os.Exit(1)
	} else {
		fmt.Println("End!")
	}
}
