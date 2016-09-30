
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.IO;
using C5;
using C5.concurrent;

namespace Benchmark
{
    class Benchmark
    {
        //IBenchmarkable<Integer></Integer> Integer> dictionary;
        IConcurrentPriorityQueue<int> dictionary;//initialize data structure
        Barrier barrier; //Enables multiple tasks to cooperatively work on an algorithm in parallel through multiple phases.

        private static Logger datafile;
        private static Logger gnuPlotScript;
        private static Logger runGnuPlot = new Logger("runGnuPlot.sh");
        private static Int64 maxThroughput;
        private static BenchmarkConfig config = new BenchmarkConfig();
        private static string timestamp;

        public static void Main(String[] args)
        {
            Console.WriteLine(SystemInfo()); // Print system info

            // Create a config object and set the values
            // TEST RUN
            config.WarmupRuns = 2;
            config.Threads = new int[] { 1, 2, 4, 6, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30 };
            config.NumberOfElements = new int[] { 10000, 100000 };
            config.MinRuns = 3;
            config.SecondsPerTest = 10;
            config.StartRangeRandom = 0;
            config.PercentageInsert = new int[] { 0, 20, 35 };
            config.PercentageDelete = new int[] { 0, 10, 15 };
            config.Prefill = true;

            // CONFIG FOR HUGE TEST - Expected to take around 5 hours, should ask for 6 just in case
            config.WarmupRuns = 4;
            //config.Threads = new int[] { 1, 3, 6, 9, 12, 15, 18, 21, 24, 30, 36, 42, 48, 54, 60 };
            //config.Threads = new int[] { 1, 2 };
            config.NumberOfElements = new int[] { 100000, 1000000 };
            config.MinRuns = 3;
            config.SecondsPerTest = 120;
            config.SecondsPerTest = 2;
            config.StartRangeRandom = 0;
            config.PercentageInsert = new int[] { 0, 20, 35, 45 };
            config.PercentageDelete = new int[] { 0, 10, 15, 40 };
            config.Prefill = true;

            config.TestConcurrentIntervalHeap = true;

            RunBenchmark();// Run the benchmark

            // StringComparer Test
            // IMPORTANT: DONE THIS WAY DUE TO STRINGCOMPARER NOT WORKING WITH GENERIC TYPES (OBVIOUSLY)
            RunStringComparerTest();

        }

        /// <summary>
        /// A benchmark test for string comparer.
        /// No parrelization.
        /// </summary>
        static void RunStringComparerTest()
        {
            var timer = new System.Diagnostics.Stopwatch();

            IConcurrentPriorityQueue<string> standardDictionary;
            IConcurrentPriorityQueue<string> customDictionary;

            var randomIntQueue = generateRandomQueue(1000000, 0, 1000000);
            var standardList = new List<string>();
            var customList = new List<string>();


            while (randomIntQueue.Count > 0)
            {
                var element = randomIntQueue.Dequeue().ToString();
                standardList.Add(element);
                customList.Add(element);
            }

            customDictionary = new GlobalLockDEPQ<string>();

            timer.Reset();

            timer.Start();
            foreach (string element in customList)
            {
                customDictionary.Add(element);
            }
            timer.Stop();
            Console.WriteLine("Custom time Add:   " + timer.ElapsedTicks);

            timer.Reset();

            timer.Start();
            foreach (string element in customList)
            {
                customDictionary.DeleteMax();
            }
            timer.Stop();
            Console.WriteLine("Custom time Get:   " + timer.ElapsedTicks);

            standardDictionary = new GlobalLockDEPQ<string>();

            timer.Reset();

            timer.Start();
            foreach (string element in standardList)
            {
                standardDictionary.Add(element);
            }
            timer.Stop();
            Console.WriteLine("Standard time Add: " + timer.ElapsedTicks);

            timer.Reset();

            timer.Start();
            foreach (string element in standardList)
            {
                standardDictionary.DeleteMin();
            }
            timer.Stop();
            Console.WriteLine("Standard time Get: " + timer.ElapsedTicks);


        }

        static void RunBenchmark()
        {
            DateTime now = DateTime.Now;
            timestamp = now.Year.ToString() + now.Month.ToString() + now.Day.ToString() + "-" + now.Hour.ToString() + now.Minute.ToString() + now.Second.ToString();

            for (int i = 0; i < config.PercentageInsert.Length; i++)//Run the desired tests and log to the log, gnuplotPrint and gnuplotScript
            {
                foreach (int elements in config.NumberOfElements)
                {
                    maxThroughput = 0;
                    config.EndRangeRandom = elements;
                    config.CurrentNumberOfElements = elements;
                    config.CurrentPercentageInsert = config.PercentageInsert[i];
                    config.CurrentPercentageDelete = config.PercentageDelete[i];
                    config.CurrentPercentageGet = 100 - config.PercentageInsert[i] - config.PercentageDelete[i];

                    string datafileName = timestamp + "-" + "datafile-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete + ".dat";
                    string gnuPlotScriptName = timestamp + "-" + "gnuPlotScript-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete + ".gp";
                    string benchmarkingResultsName = timestamp + "-" + "Benchmarking-Results-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete + ".png";

                    //log = new Logger("log-" + DateTime.Now.ToFileTime());
                    datafile = new Logger(datafileName);
                    gnuPlotScript = new Logger(gnuPlotScriptName);

                    datafile.Log(SystemInfo());
                    datafile.Log("\n" + config);

                    int numberOfTests = 0;

                    if (config.TestConcurrentIntervalHeap)
                    {
                        datafile.Log("\n\n" + "IntervalHeap");
                        Console.WriteLine("IntervalHeap" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                        new Benchmark().BenchMark(config, typeof(GlobalLockDEPQ<int>));
                        numberOfTests += 1;
                    }

                    datafile.Close();

                    gnuPlotScript.Log("set title \"Abacus - " + config.CurrentPercentageInsert + "% Insert / " + config.CurrentPercentageDelete + "% Delete / " + config.CurrentPercentageGet + "% Find" + "\"");
                    gnuPlotScript.Log("set terminal png truecolor size 800,600");
                    gnuPlotScript.Log("set xlabel \"Threads\"");
                    gnuPlotScript.Log("set ylabel \"Throughput\"");
                    gnuPlotScript.Log("set xrange [" + (config.Threads[0] - 1) + ":" + (config.Threads[config.Threads.Length - 1] + 1) + "]");
                    gnuPlotScript.Log("set yrange [0:" + (maxThroughput * 1.4) + "]");
                    gnuPlotScript.Log("set output \'" + benchmarkingResultsName + "\'");
                    gnuPlotScript.Log("plot for [IDX=0:" + (numberOfTests - 1) + "] '" + datafileName + "' i IDX u 1:2 w lines title columnheader(1)");
                    gnuPlotScript.Close();

                    runGnuPlot.Log("gnuplot '" + gnuPlotScriptName + "'");
                    runGnuPlot.Log("open " + benchmarkingResultsName);
                }
            }
            runGnuPlot.Close();
        }

        /// <summary>
        /// Main benchmarking method
        /// </summary>
        /// <param name="config"></param>
        /// <param name="type"></param>
        void BenchMark(BenchmarkConfig config, Type type)
        {
            var SecondsPerTestTimer = new System.Diagnostics.Stopwatch();

            //Run the benchmark for all the number of threads specified 
            foreach (int threadsToRun in config.Threads)
            {
                SecondsPerTestTimer.Reset();
                var tempThroughPut = new ArrayList<Int64>();
                Int64 throughput = 0;
                int runs = 0;

                //Inner loop that runs until standard deviation is below some threshold or it has done too many runs and throws an exception
                while ((SecondsPerTestTimer.ElapsedMilliseconds / 1000.0) < ((config.SecondsPerTest * 1.0) / config.Threads[config.Threads.Length - 1]) || runs <= ((config.WarmupRuns) + config.Threads[0]))
                {
                    //dictionary = (IConcurrentPriorityQueue<int>)Activator.CreateInstance(type);
                    dictionary = new GlobalLockDEPQ<int>(); // Create the correct dictionary for this run
                    
                    // Get tree to correct size before we start, if applicable.  
                    if (config.Prefill)
                    {

                        /* Each trial should start with trees that are prefilled to their expected size in the steady state, 
                        * so that you are measuring steady state performance, and not inconsistent performance as the size 
                        * of the tree is changing. If your experiment consists of random operations in the proportions 
                        * i% insertions, d% deletions, and s% searches on keys drawn uniformly randomly from a key range of 
                        * size r, then the expected size of the tree in the steady state will be ri/(i+d)
                        */
                        int r = config.EndRangeRandom - config.StartRangeRandom;
                        int steadyStateSize = config.CurrentNumberOfElements / 2;

                        if (config.CurrentPercentageInsert > 0 && config.CurrentPercentageDelete > 0)
                        {
                            steadyStateSize = (r * config.CurrentPercentageInsert) / (config.CurrentPercentageInsert + config.CurrentPercentageDelete);
                        }

                        if (steadyStateSize > r)
                            throw new Exception("Range of numbers is too small to reach steady state");

                        Queue<int> threadQueue = generateRandomQueue(steadyStateSize, config.StartRangeRandom, config.EndRangeRandom);
                        while (threadQueue.Count > 0)
                        {
                            int element = threadQueue.Dequeue();
                            dictionary.Add(element);
                        }
                    }

                    // Create the start and end barrier
                    barrier = new Barrier(threadsToRun + 1);
                    
                    // Submit the work
                    for (int threads = 1; threads <= threadsToRun; threads++)
                    {
                        int start = config.StartRangeRandom;
                        int end = config.EndRangeRandom;
                        Queue<int> threadQueue = generateRandomQueue(config.CurrentNumberOfElements, start, end);
                        Thread thread = new Thread(new ParameterizedThreadStart(Work));
                        thread.Start(threadQueue);
                    }

                    barrier.SignalAndWait();// Wait for all tasks / threads to be ready to begin work

                   
                    var t = new System.Diagnostics.Stopwatch(); // Start the timers
                    t.Start();
                    SecondsPerTestTimer.Start();

                    barrier.SignalAndWait(); // Wait for all tasks / threads to be finished with their work. Unlike Java, no need to reset the barrier in C#

                    double time = t.ElapsedTicks;// Get elapsed time
                    SecondsPerTestTimer.Stop();
                   
                    if (runs > config.WarmupRuns) // Only add results after the warmup runs
                    {
                        Int64 toAdd = (Int64)(((config.CurrentNumberOfElements * threadsToRun) / time) * 1000.0 * 10000.0);
                        tempThroughPut.Add(toAdd);
                    }
                    
                    runs++;// Increment number of runs
                }

                throughput = (Int64)tempThroughPut.Average();

                if (throughput > maxThroughput)
                {
                    maxThroughput = throughput;
                }
                datafile.Log(threadsToRun + " " + throughput);
            }
        }


        /// <summary>
        /// Generates a list of random ints.
        /// </summary>
        /// <param name="size"></param>
        /// <param name="RandomStartRange"></param>
        /// <param name="RandomEndRange"></param>
        /// <returns></returns>
        static Queue<int> generateRandomQueue(int size, int RandomStartRange, int RandomEndRange)
        {
            Random rand = new Random();
            Queue<int> queueToReturn = new Queue<int>();
            for (int i = RandomStartRange; i < (size + RandomStartRange); i++)
            {
                int randomInt = rand.Next(RandomStartRange, RandomEndRange);
                queueToReturn.Enqueue(randomInt);
            }
            return queueToReturn;
        }

        /// <summary>
        /// The work done by each thread
        /// - It writes the cubed value of each element in the queue to a disposable file in the folder specified as OUTPUT_FOLDER.
        /// The writing to the file is done in order to take time and ensure that the code is not optimized away by the compiler.
        /// In the real benchmarking version this would not be necessary.
        /// </summary>
        /// <param name="state"></param>
        private void Work(object state)
        {
            Queue<int> threadQueue = (Queue<int>)state; // Cast the state to a Queue.
            barrier.SignalAndWait();// Ensure that tasks are submitted and ready to begin work at the same time
            Random random = new Random();// Do some work, write to file to avoid the compiler recognizing the dead code.
            while (threadQueue.Count > 0)
            {
                int option = random.Next(0, 100);
                var element = threadQueue.Dequeue();

                if (option >= (100 - config.CurrentPercentageInsert))
                {
                    dictionary.Add(element);
                }
                else if (option >= (100 - config.CurrentPercentageInsert - config.CurrentPercentageGet))
                {
                    dictionary.FindMin();
                }
                else
                {
                    dictionary.DeleteMin();
                }
            }
            barrier.SignalAndWait();// Let main thread know we are done
        }


        class Logger
        {
            private readonly StreamWriter logFileWriter;

            public Logger(String fileName)
            {
                logFileWriter = new StreamWriter(fileName, false);
            }

            public void Log(String line)
            {
                logFileWriter.WriteLine(line);
            }

            public void Close()
            {
                logFileWriter.Close();
            }
        }

        private static string SystemInfo()
        {
            return
            "# OS:          " + Environment.OSVersion.VersionString + System.Environment.NewLine +
            "# .NET vers:   " + Environment.Version + System.Environment.NewLine +
            "# 64-bit OS:   " + Environment.Is64BitOperatingSystem + System.Environment.NewLine +
            "# 64-bit proc: " + Environment.Is64BitProcess + System.Environment.NewLine +
            "# CPU:         " + Environment.GetEnvironmentVariable("PROCESSOR_IDENTIFIER") + "; "
            + Environment.ProcessorCount + " cores" + System.Environment.NewLine +
            "# Date:        " + DateTime.Now;
        }

    }

    class BenchmarkConfig
    {

        public override string ToString()
        {
            return string.Format(
            "# WarmupRuns:        {0} \n" +
            "# Elements:          {1} \n" +
            "# SecondsPerTest:    {2} \n" +
            "# StartRangeRandom:  {3} \n" +
            "# EndRangeRandom:    {4} \n" +
            "# Insert%:           {5} \n" +
            "# Delete%:           {6} \n" +
            "# Get%:              {7} \n" +
            "# Prefill:           {8}",
            WarmupRuns,
            CurrentNumberOfElements,
            SecondsPerTest,
            StartRangeRandom,
            EndRangeRandom,
            CurrentPercentageInsert,
            CurrentPercentageDelete,
            CurrentPercentageGet,
            Prefill
            );
        }

        public int CurrentNumberOfElements
        {
            get;
            set;
        }

        public int MinRuns
        {
            get;
            set;
        }

        public bool TestConcurrentRBTreeBesa
        {
            get;
            set;
        }

        public int SecondsPerTest
        {
            get;
            set;
        }


        public bool Prefill
        {
            get;
            set;
        }

        public int[] PercentageDelete
        {
            get;
            set;
        }

        public int CurrentPercentageDelete
        {
            get;
            set;
        }

        public int CurrentPercentageGet
        {
            get;
            set;
        }

        public int[] PercentageInsert
        {
            get;
            set;
        }

        public int CurrentPercentageInsert
        {
            get;
            set;
        }

        public bool TestConcurrentIntervalHeap
        {
            get;
            set;
        }

        public int WarmupRuns
        {
            get;
            set;
        }

        public int[] Threads
        {
            get;
            set;
        }

        public int[] NumberOfElements
        {
            get;
            set;
        }

        public int StartRangeRandom
        {
            get;
            set;
        }

        public int EndRangeRandom
        {
            get;
            set;
        }
    }
}