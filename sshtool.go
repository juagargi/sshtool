package main

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
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
	machines         = []target{}
	summarizedOutput = make(map[string][]int) // output to machine index
	verbose          = false
	sshCommand       = "ssh"
)

type target struct {
	host string
	done bool
}

func main() {
	var commands []string
	script := ""
	command := ""
	scriptArgs := []string{}
	sshOptions := []string{}
	pathToCopy := ""
	targets := defaultTargetsFilename
	replaceInSummary := true
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--help" || os.Args[i] == "-h" {
			usage()
			return
		} else if os.Args[i] == "--verbose" || os.Args[i] == "-v" {
			verbose = true
		} else if os.Args[i] == "-t" {
			if len(os.Args) < i+2 {
				usage()
				return
			}
			targets = os.Args[i+1]
			i++
		} else if os.Args[i] == "-o" {
			if len(os.Args) < i+2 {
				usage()
				return
			}
			sshOptions = append(sshOptions, "-o", os.Args[i+1])
			i++
		} else if os.Args[i] == "-i" {
			if len(os.Args) < i+2 {
				usage()
				return
			}
			sshOptions = append(sshOptions, "-i", os.Args[i+1])
			i++
		} else if os.Args[i] == "-c" {
			if len(os.Args) < i+2 {
				usage()
				return
			}
			pathToCopy = os.Args[i+1]
			i++
		} else if os.Args[i] == "-f" {
			if len(os.Args) < i+2 || len(commands) > 0 {
				usage()
				return
			}
			script = os.Args[i+1]
			i++
		} else if os.Args[i] == "--sshcommand" {
			if len(os.Args) < i+2 {
				usage()
				return
			}
			sshCommand = os.Args[i+1]
			i++
		} else if os.Args[i] == "--verbatim" {
			replaceInSummary = false
		} else {
			if script != "" {
				scriptArgs = append(scriptArgs, fmt.Sprintf("\"%s\"", os.Args[i]))
			} else {
				commands = append(commands, os.Args[i])
			}
		}
	}
	if script == "" && len(commands) == 0 && pathToCopy == "" {
		usage()
		return
	}
	if script == "" && len(commands) > 0 {
		command = strings.Join(commands, ";")
		// amend command:
		command = "[ -f ~/.profile ] && . ~/.profile;" + command
	} else if _, err := os.Stat(script); script != "" && err != nil {
		fmt.Println("Error with script file:", err)
		os.Exit(1)
	}

	machines = loadMachines(targets)

	tempDir, err := ioutil.TempDir("", "__forallASes_temp_")
	if err != nil {
		fmt.Println("Error creating temporary directory:", err)
		os.Exit(10)
	}

	signalChannel := make(chan os.Signal, 1)
	// cleanupDone := make(chan bool)
	signal.Notify(signalChannel, os.Interrupt)
	go func() {
		for s := range signalChannel {
			handleInterrupt(s, replaceInSummary)
		}
	}()

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
		if pathToCopy != "" {
			err = remoteCopySrcToDst(&machines[i], pathToCopy, "/tmp/"+filepath.Base(pathToCopy))
		}
		if err != nil {
			close(outputs[i])
			errors[i] <- err
			close(errors[i])
		} else {
			if script != "" {
				err = runScript(&machines[i], sshOptions, script, scriptArgs, outputs[i], errors[i])
			} else if command != "" {
				err = ssh(&machines[i], sshOptions, command, outputs[i], errors[i])
			} else {
				close(outputs[i])
				close(errors[i])
			}
			if err != nil {
				close(outputs[i])
				errors[i] <- err
				close(errors[i])
			}
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
			str := allOfChannelWithTempFile(outputs[i], tempFiles[i])
			if replaceInSummary {
				// replace the occurrences of the machine names with SSHTOOL_TARGET, to unclutter output
				str = strings.Replace(str, machines[i].host, "\"$SSHTOOL_TARGET\"", -1)
			}
			output[i] = str
			sync <- i
		}(i)
	}
	// execution has finished here for all targets

	for i := 0; i < len(machines); i++ {
		machineIdx := <-sync
		machines[machineIdx].done = true
		out := output[machineIdx]
		summarizedOutput[out] = append(summarizedOutput[out], machineIdx)
		fmt.Printf("    Done %d / %d        Machine %s \n", i+1, len(machines), machines[machineIdx].host)
	}
	printSummary("Output", replaceInSummary)

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
			// replace the occurrences of the machine names with SSHTOOL_TARGET, to unclutter output
			str := x.Error()
			if replaceInSummary {
				str = strings.Replace(str, machines[i].host, "\"$SSHTOOL_TARGET\"", -1)
			}
			output[i] += str
		}
	}
	for i, msgs := range output {
		if msgs != "" {
			printErrorHeader()
			// summarize the errors
			msgs += "\n"
			summarizedOutput[msgs] = append(summarizedOutput[msgs], i)
		}
	}
	printSummary("ERROR", replaceInSummary)

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

func usage() {
	fmt.Printf(`Usage: sshtool [--verbatim] [--verbose | -v] -t TARGETS -o OPTS -i IDENT_FILE [-c FILE_OR_DIR] CMDS
Executes CMDS commands in the TARGETS targets, with ssh options OPTS.

CMDS           {'commands && to be executed' | -f script_file_here_to_run_there.sh [argument1 argument2 ...]}
TARGETS        {targets_file | 'target1,target2,...'}
OPTS           {ssh_options}
IDENT_FILE     identity file passed to ssh with -i
FILE_OR_DIR    File or directory to copy to targets. It will be copied to target:/tmp/$FILE_OR_DIR
--verbatim     Don't do summary replacements with the target names
--verbose      Be verbose when outputting
--sshcommmand  Command to run ssh; defaults to "ssh"

If -t is not specified, the target machines file will be %s . The targets file must contain a "Host" entry per target.
On each target, the environment variable SSHTOOL_TARGET will be defined with the name of the target.

Examples:
sshtool -t 'as1-11,as1-12' -f myscript.sh arg1
sshtool -o ConnectTimeout=1 -o ConnectionAttempts=1 -t ~/scionlabTargets.all 'cd $SC; ./scion.sh status'
sshtool -t as1-17 -c $SC/gen 'cd $SC; mv /tmp/gen gen.nextversion'
`, defaultTargetsFilename)
}

func loadMachinesFromLines(lines []string) []target {
	var machines []target
	separator := regexp.MustCompile(`[\s:]+`)
	for lineNumber := 1; lineNumber <= len(lines); lineNumber++ {
		line := lines[lineNumber-1]
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		fields := separator.Split(line, -1)
		switch len(fields) {
		case 0:
			continue
		case 1:
			machines = append(machines, target{host: fields[0], done: false})
		default:
			fmt.Println("Error parsing the targets file at line", lineNumber, ", expected host but encountered", len(fields), " fields instead:", line)
			os.Exit(1)
		}
	}
	return machines
}

func loadMachinesFromFile(lines []string) []target {
	machines := make([]target, 0)
	// Match lines starting with "Host "
	re := regexp.MustCompile(`^\s*[hH]ost\s+(.*)$`)

	// Find all matches
	for _, line := range lines {
		m := re.FindStringSubmatch(line)
		if len(m) > 1 {
			machines = append(machines, target{
				host: m[1],
				done: false,
			})
		}
	}
	return machines
}

func loadMachines(targets string) []target {
	// file or target list?
	if verbose {
		fmt.Printf("[sshtool] Loading targets from %s\n", targets)
	}
	var lines []string
	if _, err := os.Stat(targets); err != nil {
		// List of targets.
		lines = strings.Split(targets, ",")
		return loadMachinesFromLines(lines)
	} else {
		// File.
		file, err := os.Open(targets)
		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}
		scanner := bufio.NewScanner(file)
		for lineNumber := 1; scanner.Scan(); lineNumber++ {
			lines = append(lines, scanner.Text())
		}
		err = scanner.Err()
		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}
		return loadMachinesFromFile(lines)
	}
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

// ssh is an asynchronous function: it will run in the background reading stdout and stderr
// ssh returns an error if unable to start the ssh connection
func ssh(machine *target, sshOptions []string, command string, output chan<- string, errors chan<- error) error {
	sshOptions = append(sshOptions, "-o", "LogLevel=QUIET")

	// export an environment variable per target with its name:
	command = "export LC_ALL=C; export SSHTOOL_TARGET=\"" + machine.host + "\";" + command
	sshOptions = append(sshOptions, "-t", machine.host, command)
	cmd := exec.Command(sshCommand, sshOptions...)
	if verbose {
		fmt.Printf("[sshtool] CMD = %s\n", strings.Join(cmd.Args, " "))
	}

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

// returns output, error
func synchronousSSH(machine *target, command string) ([]string, error) {
	output := []string{}
	stdout := make(chan string)
	stderr := make(chan error)
	err := ssh(machine, []string{}, command, stdout, stderr)
	if err != nil {
		return output, err
	}
	// block until done (quit if errors)
	for err := range stderr {
		return output, err
	}
	for str := range stdout {
		output = append(output, str)
	}
	return output, nil
}

func getUniqueScriptName(script string) string {
	uniqueStr, err := makeUUID()
	if err != nil {
		uniqueStr = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	name := fmt.Sprintf("__sshtool_%s_%s", uniqueStr, filepath.Base(script))
	if verbose {
		fmt.Printf("[sshtool] script name is %s\n", name)
	}
	return name
}

func remoteCopySrcToDst(machine *target, srcPath, dstPath string) error {
	// remove possibly existing target
	if !strings.HasPrefix(dstPath, "/tmp/") {
		// it's too dangerous otherwise to remove anything like we do
		return fmt.Errorf("SSHTOOL internal: cowardly refusing to remove and copy to anywhere but /tmp/")
	}
	_, err := synchronousSSH(machine, "rm -rf "+dstPath)
	if err != nil {
		return err
	}
	cmd := exec.Command("scp", "-r", srcPath, machine.host+":"+dstPath)
	if verbose {
		fmt.Printf("[sshtool] copy file CMD = %s\n", strings.Join(cmd.Args, " "))
	}
	return cmd.Run()
}

func runScript(machine *target, sshOptions []string, script string, scriptArgs []string, output chan<- string, errors chan<- error) error {
	go func() {
		remoteScript := getUniqueScriptName(script)
		err := remoteCopySrcToDst(machine, script, "/tmp/"+remoteScript)
		if err != nil {
			close(output)
			errors <- err
			close(errors)
			return
		}
		// susceptible to injection, but it's okay as we allow execution of anything anyways:
		scriptLine := "/tmp/" + remoteScript + " " + strings.Join(scriptArgs, " ") + ";EX=$?"
		err = ssh(machine, sshOptions, "cd /tmp;chmod +x "+remoteScript+";. ~/.profile;"+scriptLine+";rm "+remoteScript+";exit $EX", output, errors)
		if err != nil {
			close(output)
			errors <- err
			close(errors)
		}
	}()
	return nil
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

func printSummary(heading string, summarize bool) {
	var length int
	if summarize {
		length = len(summarizedOutput)
	} else {
		length = len(machines)
	}
	outputIndex := 1
	for k, v := range summarizedOutput {
		var theseHosts [][]int
		if summarize {
			theseHosts = [][]int{v}
		} else {
			for _, i := range v {
				theseHosts = append(theseHosts, []int{i})
			}
		}
		for _, hostsWithThisOutput := range theseHosts {
			fmt.Println("-----------------------------------------------")
			fmt.Printf("-- %s %d / %d :\n", heading, outputIndex, length)
			fmt.Println("---- BEGIN -----------------------------------")
			fmt.Print(k)
			fmt.Println("---- END --------------------------------------")
			fmt.Println("For targets:")
			for _, i := range hostsWithThisOutput {
				fmt.Printf("%v ", machines[i].host)
			}
			fmt.Println()
			outputIndex++
		}
	}
	fmt.Println("-----------------------------------------------")
}

func handleInterrupt(sig os.Signal, summarize bool) {
	// print status
	found := []int{}
	for i, m := range machines {
		if !m.done {
			found = append(found, i)
		}
	}
	fmt.Printf("\n%d pending jobs\n", len(found))
	for _, i := range found {
		fmt.Printf("%v\n", machines[i].host)
	}
	// confirm abort with user
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Abort? (y/n) ")
	text, _ := reader.ReadString('\n')
	text = text[:len(text)-1]
	if text == "y" {
		printSummary("Partial Output", summarize)
		os.Exit(100)
	}
}

func makeUUID() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
