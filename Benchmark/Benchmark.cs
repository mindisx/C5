
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
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
        private static string dir = "output";

        public static void Main(String[] args)
        {

            Directory.CreateDirectory(dir); //create outut folder
            Console.WriteLine(SystemInfo()); // Print system info

            // Create a config object and set the values
            // TEST RUN
            config.WarmupRuns = 2;
            config.Threads = new[] { 1, 2, 4, 6, 8 };
            config.NumberOfElements = new[] { 100000 };
            config.MinRuns = 3;
            config.SecondsPerTest = 10;
            config.StartRangeRandom = 0;
            config.PercentageInsert = new[] { 20, 35 };
            config.PercentageDeleteMin = new[] { 10, 15 };
            config.PercentageDeleteMax = new[] { 10, 15 };
            config.Prefill = true;

            //CONFIG FOR HUGE TEST - Expected to take around 5 hours, should ask for 6 just in case
            //config.WarmupRuns = 4;
            //config.Threads = new int[] { 1, 3, 6, 9, 12, 15, 18, 21, 24, 30, 36, 42, 48, 54, 60 };
            //config.Threads = new int[] { 1, 2 };
            //config.NumberOfElements = new[] { 100000, 1000000 };
            //config.MinRuns = 3;
            //config.SecondsPerTest = 120;
            //config.SecondsPerTest = 2;
            //config.StartRangeRandom = 0;
            //config.PercentageInsert = new[] { 0, 20, 35, 45 };
            //config.PercentageDelete = new[] { 0, 10, 15, 40 };
            //config.Prefill = true;

            config.GlobalLockDEPQ = false;
            config.HuntLockDEPQv1 = false;
            config.HuntLockDEPQv2 = false;
            config.HuntLockDEPQv3 = false;
            config.GlobalLockSkipList = true;
            config.LothanShavitSkipList = false;
            config.HellerSkipListv1 = true;
            config.HellerSkipListv2 = true;


            RunBenchmark();// Run the benchmark

            // StringComparer Test
            // IMPORTANT: DONE THIS WAY DUE TO STRINGCOMPARER NOT WORKING WITH GENERIC TYPES (OBVIOUSLY)
            //RunStringComparerTest();

        }

        /// <summary>
        /// A benchmark test for string comparer.
        /// No parrelization.
        /// </summary>
        static void RunStringComparerTest()
        {
            var timer = new Stopwatch();

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
            #region custom
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
            #endregion

            #region standard
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

            #endregion



        }

        /// <summary>
        /// 
        /// </summary>
        static void RunBenchmark()
        {
            DateTime now = DateTime.Now;
            timestamp = now.Year + now.Month.ToString() + now.Day + "-" + now.Hour + now.Minute + now.Second;

            for (int i = 0; i < config.PercentageInsert.Length; i++)//Run the desired tests and log to the log, gnuplotPrint and gnuplotScript
            {
                foreach (int elements in config.NumberOfElements)
                {
                    maxThroughput = 0;
                    config.EndRangeRandom = elements;
                    config.CurrentNumberOfElements = elements;
                    config.CurrentPercentageInsert = config.PercentageInsert[i];
                    config.CurrentPercentageDeleteMin = config.PercentageDeleteMin[i];
                    config.CurrentPercentageDeleteMax = config.PercentageDeleteMax[i];
                    config.CurrentPercentageGetMin = (100 - config.PercentageInsert[i] - config.PercentageDeleteMax[i] - config.PercentageDeleteMax[i]) / 2;
                    config.CurrentPercentageGetMax = (100 - config.PercentageInsert[i] - config.PercentageDeleteMax[i] - config.PercentageDeleteMax[i] - config.CurrentPercentageGetMin);


                    string datafileName = timestamp + "-" + "datafile-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax + ".dat";
                    string gnuPlotScriptName = timestamp + "-" + "gnuPlotScript-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax + ".gp";
                    string benchmarkingResultsName = timestamp + "-" + "Benchmarking-Results-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax + ".png";

                    //log = new Logger("log-" + DateTime.Now.ToFileTime());
                    datafile = new Logger(dir + "/" + datafileName);
                    gnuPlotScript = new Logger(dir + "/" + gnuPlotScriptName);

                    datafile.Log(SystemInfo());
                    datafile.Log("\n" + config);

                    int numberOfTests = 0;

                    if (config.GlobalLockDEPQ)
                    {
                        datafile.Log("\n\n" + "GlobalLockDEPQ");
                        new Benchmark().BenchMark(config, typeof(GlobalLockDEPQ<int>));
                        Console.WriteLine("GlobalLockDEPQ  " + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax);
                        Console.WriteLine("Execution Time: " + config.ExecutionTime);
                        numberOfTests += 1;
                    }

                    if (config.HuntLockDEPQv1)
                    {
                        datafile.Log("\n\n" + "HuntLockDEPQv1");
                        new Benchmark().BenchMark(config, typeof(HuntLockDEPQv1<int>));
                        Console.WriteLine("HuntLockDEPQv1 " + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax);
                        Console.WriteLine("Execution Time: " + config.ExecutionTime);
                        numberOfTests += 1;
                    }

                    if (config.HuntLockDEPQv2)
                    {
                        datafile.Log("\n\n" + "HuntLockDEPQv2");
                        new Benchmark().BenchMark(config, typeof(HuntLockDEPQv2<int>));
                        Console.WriteLine("HuntLockDEPQv2 " + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax);
                        Console.WriteLine("Execution Time: " + config.ExecutionTime);
                        numberOfTests += 1;
                    }

                    if (config.HuntLockDEPQv3)
                    {
                        datafile.Log("\n\n" + "HuntLockDEPQv3");
                        new Benchmark().BenchMark(config, typeof(HuntLockDEPQv3<int>));
                        Console.WriteLine("HuntLockDEPQv3 " + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax);
                        Console.WriteLine("Execution Time: " + config.ExecutionTime);
                        numberOfTests += 1;
                    }

                    if (config.GlobalLockSkipList)
                    {
                        datafile.Log("\n\n" + "GlobalLockSkipList");
                        new Benchmark().BenchMark(config, typeof(GlobalLockSkipList<int>));
                        Console.WriteLine("GlobalLockSkipList " + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax);
                        Console.WriteLine("Execution Time: " + config.ExecutionTime);
                        numberOfTests += 1;
                    }

                    if (config.LothanShavitSkipList)
                    {
                        datafile.Log("\n\n" + "LothanShavitSkipList");
                        new Benchmark().BenchMark(config, typeof(LotanShavitSkiplist<int>));
                        Console.WriteLine("LothanShavitSkipList " + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax);
                        Console.WriteLine("Execution Time: " + config.ExecutionTime);
                        numberOfTests += 1;
                    }


                    if (config.HellerSkipListv1)
                    {
                        datafile.Log("\n\n" + "HellerSkipListv1");
                        new Benchmark().BenchMark(config, typeof(HellerSkipListv1<int>));
                        Console.WriteLine("HellerSkipListv1 " + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax);
                        Console.WriteLine("Execution Time: " + config.ExecutionTime);
                        numberOfTests += 1;
                    }

                    if (config.HellerSkipListv2)
                    {
                        datafile.Log("\n\n" + "HellerSkipListv2");
                        new Benchmark().BenchMark(config, typeof(HellerSkipListv2<int>));
                        Console.WriteLine("HellerSkipListv2 " + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDeleteMin + "_" + config.CurrentPercentageDeleteMax);
                        Console.WriteLine("Execution Time: " + config.ExecutionTime);
                        numberOfTests += 1;
                    }
                    datafile.Close();

                    gnuPlotScript.Log("set title \"Abacus - " + config.CurrentPercentageInsert + "% Insert / " + config.CurrentPercentageDeleteMax + "% DeleteMax / " + config.CurrentPercentageDeleteMin + "% DeleteMin / " + config.CurrentPercentageGetMin + "% FindMin /" + +config.CurrentPercentageGetMax + "% FindMax" + "/");
                    gnuPlotScript.Log("set terminal png truecolor size 800,600");
                    gnuPlotScript.Log("set xlabel \"Threads\"");
                    gnuPlotScript.Log("set ylabel \"Throughput\"");
                    gnuPlotScript.Log("set xrange [" + (config.Threads[0] - 1) + ":" + (config.Threads[config.Threads.Length - 1] + 1) + "]");
                    //change decimal seperator from , to . if this conversion not performed, gnuplot script will not run
                    gnuPlotScript.Log("set yrange [0:" + (maxThroughput * 1.4).ToString("G", CultureInfo.CreateSpecificCulture("en-US")) + "]");
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
            var SecondsPerTestTimer = new Stopwatch();

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
                    dictionary = (IConcurrentPriorityQueue<int>)Activator.CreateInstance(type);
                    //dictionary = new GlobalLockDEPQ<int>(); // Create the correct dictionary for this run

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

                        if (config.CurrentPercentageInsert > 0 && config.CurrentPercentageDeleteMin > 0 && config.CurrentPercentageDeleteMax > 0)
                        {
                            steadyStateSize = (r * config.CurrentPercentageInsert) / (config.CurrentPercentageInsert + config.CurrentPercentageDeleteMin + config.CurrentPercentageDeleteMax);
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
                        Thread thread = new Thread(Work);
                        thread.Start(threadQueue);
                    }

                    barrier.SignalAndWait();// Wait for all tasks / threads to be ready to begin work


                    var t = new Stopwatch(); // Start the timers
                    t.Start();
                    SecondsPerTestTimer.Start();

                    barrier.SignalAndWait(); // Wait for all tasks / threads to be finished with their work. Unlike Java, no need to reset the barrier in C#

                    double time = t.ElapsedTicks;// Get elapsed time
                    config.ExecutionTime = t.ElapsedMilliseconds;
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
                //add a line with threads and execution time and a line with threads and throughput
                //datafile.Log(threadsToRun + " " + config.ExecutionTime);
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
                else if (option >= (100 - config.CurrentPercentageInsert - config.CurrentPercentageDeleteMin))
                {
                    try
                    {
                        dictionary.DeleteMin();
                    }
                    catch (NoSuchItemException) { }
                }
                else if (option >= (100 - config.CurrentPercentageInsert - config.CurrentPercentageDeleteMin - config.CurrentPercentageDeleteMax))
                {
                    try
                    {
                        dictionary.DeleteMax();
                    }
                    catch (NoSuchItemException e) { }
                }
                else if (option >= (100 - config.CurrentPercentageInsert - config.CurrentPercentageDeleteMin - config.CurrentPercentageDeleteMax - config.CurrentPercentageGetMin))
                {
                    try
                    {
                        dictionary.FindMin();
                    }
                    catch (Exception) { }
                }
                else
                {
                    try
                    {
                        dictionary.FindMax();
                    }
                    catch (Exception) { }
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
            "# OS:          " + Environment.OSVersion.VersionString + Environment.NewLine +
            "# .NET vers:   " + Environment.Version + Environment.NewLine +
            "# 64-bit OS:   " + Environment.Is64BitOperatingSystem + Environment.NewLine +
            "# 64-bit proc: " + Environment.Is64BitProcess + Environment.NewLine +
            "# CPU:         " + Environment.GetEnvironmentVariable("PROCESSOR_IDENTIFIER") + "; "
            + Environment.ProcessorCount + " cores" + Environment.NewLine +
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
            "# DeleteMin%:        {6} \n" +
            "# DeleteMax%:		  {7} \n" +
            "# GetMin%:           {8} \n" +
            "# GetMax%:           {9} \n" +
            "# Prefill:           {10}",
            WarmupRuns,
            CurrentNumberOfElements,
            SecondsPerTest,
            StartRangeRandom,
            EndRangeRandom,
            CurrentPercentageInsert,
            CurrentPercentageDeleteMin,
            CurrentPercentageDeleteMax,
            CurrentPercentageGetMin,
            CurrentPercentageGetMax,
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

        public int[] PercentageDeleteMax
        {
            get;
            set;
        }

        public int[] PercentageDeleteMin
        {
            get;
            set;
        }

        public int CurrentPercentageDeleteMin
        {
            get;
            set;
        }

        public int CurrentPercentageDeleteMax
        {
            get;
            set;
        }

        public int CurrentPercentageGetMin
        {
            get;
            set;
        }
        public int CurrentPercentageGetMax
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

        public bool GlobalLockDEPQ
        {
            get;
            set;
        }

        public bool HuntLockDEPQv1
        {
            get;
            set;
        }

        public bool HuntLockDEPQv2
        {
            get;
            set;
        }

        public bool HuntLockDEPQv3
        {
            get;
            set;
        }

        public bool GlobalLockSkipList
        {
            get;
            set;
        }
        public bool HellerSkipListv1
        {
            get;
            set;
        }

        public bool HellerSkipListv2
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

        public long ExecutionTime { get; set; }
        public bool LothanShavitSkipList { get; internal set; }
    }
}