
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;

using System.Threading;
using System.IO;
using C5;
using C5.concurrent;

namespace Benchmark
{
    class Benchmark
    {
        //IBenchmarkable<Integer></Integer> Integer> dictionary;
        IConcurrentPriorityQueue<int> dictionary;
        //initialize data structure

        Barrier barrier; //Enables multiple tasks to cooperatively work on an algorithm in parallel through multiple phases.

        private static Logger log;
        private static Logger datafile;
        private static Logger gnuPlotScript;
        private static Logger runGnuPlot = new Logger("runGnuPlot.sh");
        private static Int64 maxThroughput;
        private static BenchmarkConfig config = new BenchmarkConfig();
        private static string timestamp;

        public static void Main(String[] args)
        {
            // Print system info
            Console.WriteLine(SystemInfo());

            // Create a config object and set the values
            // TEST RUN
            config.WarmupRuns = 2;
            config.Threads = new int[] { 1, 2, 4, 8 };
            config.NumberOfElements = new int[] { 10000, 100000 };
            config.MinRuns = 3;
            config.SecondsPerTest = 10;
            config.StartRangeRandom = 0;
            config.PercentageInsert = new int[] { 0, 20, 35 };
            config.PercentageDelete = new int[] { 0, 10, 15 };
            config.Prefill = true;

            // CONFIG FOR HUGE TEST - Expected to take around 5 hours, should ask for 6 just in case

            config.WarmupRuns = 4;
            config.Threads = new int[] { 1, 3, 6, 9, 12, 15, 18, 21, 24, 30, 36, 42, 48, 54, 60 };
            config.Threads = new int[] { 1, 2 };
            config.NumberOfElements = new int[] { 100000, 1000000 };
            config.NumberOfElements = new int[] { 10000 };
            config.MinRuns = 3;
            config.SecondsPerTest = 120;
            config.SecondsPerTest = 2;
            config.StartRangeRandom = 0;
            config.PercentageInsert = new int[] { 0, 20, 35, 45 };
            config.PercentageDelete = new int[] { 0, 10, 15, 40 };
            config.Prefill = true;

            config.TestConcurrentIntervalHeap = true;
            //config.TestChromaticTree = true;
            //config.TestChromaticRBTreeOriginal = false;
            //config.TestChromaticRBTreeOriginalWithSCXChange = false;
            //config.TestLockfreeTree = false;
            //config.TestWrappedC5Tree = false;
            //config.TestConcurrentDictionaryWrapperClass = false;
            //config.TestConcurrentRBTreeBesa = true;
            //config.TestRelaxedTree = false;


            config.TestBesaNakedReadWrite = false;
            config.TestBesaVolatileReadWrite = true;
            config.TestChromaticRBTreeOriginalAssignedDefault = false;
            config.TestChromaticTreeNoNull = true;

            // Run the benchmark
            RunBenchMark();

            // Test Integer class
            //			new IntegerClassTest().RunTest(10000000);

            // StringComparer Test
            // IMPORTANT: DONE THIS WAY DUE TO STRINGCOMPARER NOT WORKING WITH GENERIC TYPES (OBVIOUSLY)
            //			RunStringComparerTest();

        }

        static void RunStringComparerTest()
        {
            const int warmupRuns = 2;
            const int measuredRuns = 3;
            const int totalRuns = warmupRuns + measuredRuns;

            var timer = new System.Diagnostics.Stopwatch();

            IConcurrentPriorityQueue<String> standardDictionary;
            IConcurrentPriorityQueue<String> customDictionary;

            var threadQueue = generateRandomQueue(1000000, 0, 1000000);
            var standardList = new List<String>();
            var customList = new List<String>();

            while (threadQueue.Count > 0)
            {
                var element = threadQueue.Dequeue().ToString();
                standardList.Add(element);
                customList.Add(element);
            }

            customDictionary = new ConcurrentIntervalHeap<String>();
            timer.Reset();
            timer.Start();
            foreach (String element in customList)
            {
                customDictionary.Add(element);
            }
            timer.Stop();
            Console.WriteLine("Custom time Add:   " + timer.ElapsedTicks);

            timer.Reset();
            timer.Start();
            foreach (String element in customList)
            {
                customDictionary.DeleteMax();
            }
            timer.Stop();
            Console.WriteLine("Custom time Get:   " + timer.ElapsedTicks);

            standardDictionary = new ConcurrentIntervalHeap<String>();

            timer.Reset();
            timer.Start();
            foreach (String element in standardList)
            {
                standardDictionary.Add(element);
            }
            timer.Stop();
            Console.WriteLine("Standard time Add: " + timer.ElapsedTicks);

            timer.Reset();
            timer.Start();
            foreach (String element in standardList)
            {
                standardDictionary.DeleteMin();
            }
            timer.Stop();
            Console.WriteLine("Standard time Get: " + timer.ElapsedTicks);


        }

        static void RunBenchMark()
        {

            DateTime now = DateTime.Now;
            timestamp = now.Year.ToString() + now.Month.ToString() + now.Day.ToString() + "-" + now.Hour.ToString() + now.Minute.ToString() + now.Second.ToString();

            /*
			 * Run the desired tests and log to the log, gnuplotPrint and gnuplotScript
			 */

            for (int i = 0; i < config.PercentageInsert.Length; i++)
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



                    //				log = new Logger("log-" + DateTime.Now.ToFileTime());
                    datafile = new Logger(datafileName);
                    gnuPlotScript = new Logger(gnuPlotScriptName);

                    datafile.Log(SystemInfo());
                    datafile.Log("\n" + config);

                    int numberOfTests = 0;

                    if (config.TestConcurrentIntervalHeap)
                    {
                        datafile.Log("\n\n" + "IntervalHeap");
                        Console.WriteLine("IntervalHeap" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                        new Benchmark().BenchMark(config, typeof(ConcurrentIntervalHeap<int>));
                        numberOfTests += 1;
                    }
                    //if (config.TestChromaticTree)
                    //{
                    //    datafile.Log("\n\n" + "ChromaticTree");
                    //    Console.WriteLine("ChromaticRBTree-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(ChromaticTreeWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestWrappedC5Tree)
                    //{
                    //    datafile.Log("\n\n" + "C5TreeDictionary");
                    //    Console.WriteLine("C5TreeDictionary-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(TreeDictionaryWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestLockfreeTree)
                    //{
                    //    datafile.Log("\n\n" + "RBTreeParallel");
                    //    Console.WriteLine("RBTreeParallel-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(LockfreeTreeWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestConcurrentDictionaryWrapperClass)
                    //{
                    //    datafile.Log("\n\n" + ".NetConcurrentDictionary");
                    //    Console.WriteLine("NetConcurrentDictionary-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(ConcurrentDictionaryWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestChromaticRBTreeOriginal)
                    //{
                    //    datafile.Log("\n\n" + "ChromaticRBTreeOriginal");
                    //    Console.WriteLine("ChromaticRBTreeOriginal-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(ChromaticRBTreeOriginalWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestChromaticRBTreeOriginalWithSCXChange)
                    //{
                    //    datafile.Log("\n\n" + "ChromaticRBTreeOriginalWithSCXChange");
                    //    Console.WriteLine("ChromaticRBTreeOriginalWithSCXChange-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(ChromaticRBTreeOriginalWithSCXChangeWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestConcurrentRBTreeBesa)
                    //{
                    //    datafile.Log("\n\n" + "ConcurrentRBTreeBesa");
                    //    Console.WriteLine("ConcurrentRBTreeBesa-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(ConcurrentRBTreeWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestBesaVolatileReadWrite)
                    //{
                    //    datafile.Log("\n\n" + "ConcurrentRBTreeBesaV2");
                    //    Console.WriteLine("ConcurrentRBTreeBesaV2-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(BesaVolatileReadWriteWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestBesaNakedReadWrite)
                    //{
                    //    datafile.Log("\n\n" + "BesaNakedReadWrite");
                    //    Console.WriteLine("BesaNakedReadWritee-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(BesaNakedReadWriteWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestChromaticRBTreeOriginalAssignedDefault)
                    //{
                    //    datafile.Log("\n\n" + "ChromaticRBTreeOriginalAssignedDefault");
                    //    Console.WriteLine("ChromaticRBTreeOriginalAssignedDefault-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(ChromaticRBTreeOriginalAssignedDefaultWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}
                    //if (config.TestChromaticTreeNoNull)
                    //{
                    //    datafile.Log("\n\n" + "ChromaticTreeV2");
                    //    Console.WriteLine("ChromaticTreeV2-" + elements + "_" + config.CurrentPercentageInsert + "_" + config.CurrentPercentageDelete);
                    //    new Benchmark().BenchMark(config, typeof(ChromaticTreeNoNullWrapperClass<Integer, Integer>));
                    //    numberOfTests += 1;
                    //}



                    //	log.Close();
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

        /* 
         * Main benchmarking method
         */

        void BenchMark(BenchmarkConfig config, Type type)
        {
            var SecondsPerTestTimer = new System.Diagnostics.Stopwatch();

            /*
             * Run the benchmark for all the number of threads specified 
             */

            foreach (int threadsToRun in config.Threads)
            {
                SecondsPerTestTimer.Reset();
                var tempThroughPut = new ArrayList<Int64>();
                Int64 throughput = 0;
                int runs = 0;


                /*
                 * Inner loop that runs until standard deviation is below some threshold or it has done too many runs and throws an exception
                 */

                while ((SecondsPerTestTimer.ElapsedMilliseconds / 1000.0) < ((config.SecondsPerTest * 1.0) / config.Threads[config.Threads.Length - 1]) || runs <= ((config.WarmupRuns) + config.Threads[0])) {


                    // Create the correct dictionary for this run
                    //dictionary = (IConcurrentPriorityQueue<int>)Activator.CreateInstance(type);
                    dictionary = new ConcurrentIntervalHeap<int>();

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
                            //							dictionary.Add (threadQueue.Dequeue ().ToString(), System.Threading.Thread.CurrentThread.ManagedThreadId.ToString());
                            dictionary.Add(element);
                        }
                    }

                    // Create the start and end barrier
                    barrier = new Barrier(threadsToRun + 1);
                    /* 
                    Submit the work
                    */

                    for (int threads = 1; threads <= threadsToRun; threads++)
                    {
                        int start = config.StartRangeRandom;
                        int end = config.EndRangeRandom;
                        Queue<int> threadQueue = generateRandomQueue(config.CurrentNumberOfElements, start, end);
                        Thread thread = new Thread(new ParameterizedThreadStart(Work));
                        thread.Start(threadQueue);
                    }

                    // Wait for all tasks / threads to be ready to begin work
                    barrier.SignalAndWait();

                    // Start the timers
                    var t = new System.Diagnostics.Stopwatch();
                    t.Start();
                    SecondsPerTestTimer.Start();

                    // Wait for all tasks / threads to be finished with their work. Unlike Java, no need to reset the barrier in C#
                    barrier.SignalAndWait();

                    // Get elapsed time
                    double time = t.ElapsedTicks;
                    SecondsPerTestTimer.Stop();

                    // Only add results after the warmup runs
                    if (runs > config.WarmupRuns)
                    {
                        Int64 toAdd = (Int64)(((config.CurrentNumberOfElements * threadsToRun) / time) * 1000.0 * 10000.0);
                        tempThroughPut.Add(toAdd);
                    }

                    // Increment number of runs
                    runs++;

                }

                throughput = (Int64)tempThroughPut.Average();

                if (throughput > maxThroughput)
                {
                    maxThroughput = throughput;
                }
                datafile.Log(threadsToRun + " " + throughput);
            }
        }


        /*
        * Generates a queue of random ints
        */

        static Queue<int> generateRandomQueue(int size, int RandomStartRange, int RandomEndRange)
        {
            //			Console.WriteLine ("I was called with " + RandomStartRange + " and " + RandomEndRange);
            Random rand = new Random();
            Queue<int> queueToReturn = new Queue<int>();
            for (int i = RandomStartRange; i < (size + RandomStartRange); i++)
            {
                //				queueToReturn.Enqueue (i);
                int randomInt = rand.Next(RandomStartRange, RandomEndRange);
                queueToReturn.Enqueue(randomInt);
            }
            return queueToReturn;
        }


        /*
        * The work done by each thread
        * - It writes the cubed value of each element in the queue to a disposable file in the folder specified as OUTPUT_FOLDER.
        * The writing to the file is done in order to take time and ensure that the code is not optimized away by the compiler.
        * In the real benchmarking version this would not be necessary.
        */

        private void Work(Object state)
        {
            // Cast the state to a Queue.
            Queue<int> threadQueue = (Queue<int>)state;

            // Ensure that tasks are submitted and ready to begin work at the same time
            barrier.SignalAndWait();

            // Do some work, write to file to avoid the compiler recognizing the dead code.
            Random random = new Random();
            while (threadQueue.Count > 0)
            {
                int option = random.Next(0, 100);
                //				var element = threadQueue.Dequeue().ToString();
                var element = threadQueue.Dequeue();

                if (option >= (100 - config.CurrentPercentageInsert))
                {
                    dictionary.Add(element);
                    //dictionary.Add(element);
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

            // Let main thread know we are done
            barrier.SignalAndWait();
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

        private static String SystemInfo()
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

        public bool TestChromaticTreeNoNull
        {
            get;
            set;
        }

        public bool TestChromaticRBTreeOriginalAssignedDefault { get; set; }

        public bool TestBesaNakedReadWrite
        {
            get;
            set;
        }

        public bool TestBesaVolatileReadWrite
        {
            get;
            set;
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

        //public bool TestChromaticRBTreeOriginal
        //{
        //    get;
        //    set;
        //}

        //public bool TestChromaticRBTreeOriginalWithSCXChange
        //{
        //    get;
        //    set;
        //}

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

        //public bool TestConcurrentDictionaryWrapperClass
        //{
        //    get;
        //    set;
        //}

        //public bool TestRelaxedTree
        //{
        //    get;
        //    set;
        //}

        //public bool TestLockfreeTree
        //{
        //    get;
        //    set;
        //}

        //public bool TestWrappedC5Tree
        //{
        //    get;
        //    set;
        //}

        //public bool TestChromaticTree
        //{
        //    get;
        //    set;
        //}

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