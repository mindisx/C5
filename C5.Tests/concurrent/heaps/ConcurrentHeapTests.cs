/*
 Copyright (c) 2003-2016 <...insert names here...>
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 
 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.
 
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
*/

using System;
using C5;
using C5.concurrent;
using System.Collections.Generic;
using NUnit.Framework;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Linq;

namespace C5UnitTests.concurrent
{
    #region Sequential Tests
    [TestFixture]
    public class SequentialTests
    {
        IConcurrentPriorityQueue<int> queue;

        [SetUp]
        public void Init() { queue = new SkipList<int>(); }

        [TearDown]
        public void Dispose() { queue = null; }


        [Test]
        public void EmptyQueueDeleteMaxTest()
        {
            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMax());
        }

        [Test]
        public void EmptyQueueDeleteMinTest()
        {
            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMin());
        }

        [Test]
        public void EmptyQueueFindMaxTest()
        {
            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.FindMax());
        }

        [Test]
        public void EmptyQueueFindMinTest()
        {
            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.FindMin());
        }

        [Test]
        public void IsEmptyTest()
        {
            Assert.IsTrue(queue.IsEmpty());
            queue.Add(1);
            Assert.IsFalse(queue.IsEmpty());
        }

        [Test]
        public void CountTest()
        {
            Assert.AreEqual(0, queue.Count);
            queue.Add(1);
            queue.Add(2);
            queue.Add(3);
            queue.Add(4);
            Assert.AreEqual(4, queue.Count);
        }

        [Test]
        public void AddTest()
        {
            queue.Add(20);
            queue.Add(1);
            queue.Add(19);
            queue.Add(100);
            queue.Add(0);
            queue.Add(31);
            queue.Add(27);
            queue.Add(0);
            queue.Add(16);
            queue.Add(57);
            Assert.AreEqual(10, queue.Count);
        }

        [Test]
        [Repeat(1000)]
        public void RemoveMaxTest()
        {
            queue.Add(20);
            queue.Add(1);
            queue.Add(19);
            queue.Add(100);
            queue.Add(0);
            queue.Add(31);
            queue.Add(27);
            queue.Add(0);
            queue.Add(16);
            queue.Add(57);
            Assert.AreEqual(100, queue.DeleteMax());
            Assert.AreEqual(57, queue.DeleteMax());
            Assert.AreEqual(31, queue.DeleteMax());
            Assert.AreEqual(27, queue.DeleteMax());
            Assert.AreEqual(20, queue.DeleteMax());
            Assert.AreEqual(19, queue.DeleteMax());
        }

        [Test]
        [Repeat(1000)]
        public void RemoveMinTest()
        {
            queue.Add(20);
            queue.Add(1);
            queue.Add(19);
            queue.Add(100);
            queue.Add(0);
            queue.Add(31);
            queue.Add(27);
            queue.Add(0);
            queue.Add(16);
            queue.Add(57);
            Assert.AreEqual(0, queue.DeleteMin());
            Assert.AreEqual(0, queue.DeleteMin());
            Assert.AreEqual(1, queue.DeleteMin());
            Assert.AreEqual(16, queue.DeleteMin());
            Assert.AreEqual(19, queue.DeleteMin());
            Assert.AreEqual(20, queue.DeleteMin());
        }

        [Test]
        [Repeat(1000)]
        public void ManyInserts()
        {
            Random rng = new Random();
            List<int> list = new List<int>();
            int n = 10000;
            for (int i = 0; i < n; i++)
            {
                int ran = rng.Next(1000);
                list.Add(ran);
                queue.Add(ran);
            }
            Assert.AreEqual(n, queue.Count);
            list = list.OrderByDescending(i => i).ToList();
            foreach (int i in list)
            {
                Assert.AreEqual(i, queue.DeleteMax());
            }
            Assert.AreEqual(0, queue.Count);
        }

        [Test]
        public void AllTest()
        {
            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.All());

            int[] elements = new int[] { 1, 25, 7, 80, 32, 0 };

            foreach (int e in elements) { queue.Add(e); }

            int[] testElements = (int[])queue.All();
            Assert.AreEqual(elements.Length, testElements.Length);
            foreach (int e in elements)
            {
                int pos = Array.IndexOf(testElements, e);
                Assert.IsTrue(pos > -1);
            }
        }
    }

    #endregion

    #region Concurrent Tests
    /// <summary>
    /// Concurrent tests that should cover Add, RemoveMax, RemoveMin 
    /// operation by several threads.
    /// </summary>
    [TestFixture]
    class ConcurrencyTest
    {
        private IConcurrentPriorityQueue<int> queue;
        private int threadCount;
        private int n;
        private bool prefill;

        /// <summary>
        /// Test setup.
        /// Enviroment.ProcessorCount is number of logical processors
        /// </summary>
        [SetUp]
        public void Init()
        {
            queue = new HellerSkipListv2<int>();
            threadCount = Environment.ProcessorCount + 2;
            n = 10000;
            prefill = true;
        }

        [TearDown]
        public void Dispose()
        {
            queue = null;
        }


        //[Test]
        public void CountTest()
        {
            throw new NotImplementedException();
        }

        //[Test]
        public void IsEmptyTest()
        {
            throw new NoSuchItemException();
        }

        [Test]
        public void FindMaxTest()
        {
            Assert.Throws<NoSuchItemException>(() => queue.FindMax());
            Random rng = new Random();
            if (prefill)
                for (int i = 0; i < n; i++)
                    queue.Add(rng.Next(1000));
            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                Thread t = new Thread(() =>
                {
                    Random random = new Random();
                    for (int y = 0; y < n; y++)
                        queue.FindMax();
                });
                threads[i] = t;
            }

            for (int i = 0; i < threads.Length; i++)
                threads[i].Start();
            for (int i = 0; i < threads.Length; i++)
                threads[i].Join();
            Assert.IsTrue(queue.Check());
        }

        [Test]
        public void FindMinTest()
        {
            Assert.Throws<NoSuchItemException>(() => queue.FindMin());
            Random rng = new Random();
            if (prefill)
                for (int i = 0; i < n; i++)
                    queue.Add(rng.Next(1000));
            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                Thread t = new Thread(() =>
                {
                    Random random = new Random();
                    for (int y = 0; y < n; y++)
                        queue.FindMin();
                });
                threads[i] = t;
            }

            for (int i = 0; i < threads.Length; i++)
                threads[i].Start();
            for (int i = 0; i < threads.Length; i++)
                threads[i].Join();
            Assert.IsTrue(queue.Check());
        }

        [Test]
        [Repeat(10)]
        public void AddTest()
        {
            Random rng = new Random();
            if (prefill)
                for (int i = 0; i < n; i++)
                    queue.Add(rng.Next(1000));
            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                Thread t = new Thread(() =>
                {
                    Random random = new Random();
                    for (int y = 0; y < n; y++)
                        queue.Add(random.Next(1000));
                });
                threads[i] = t;
            }

            for (int i = 0; i < threads.Length; i++)
                threads[i].Start();
            for (int i = 0; i < threads.Length; i++)
                threads[i].Join();
            Assert.IsTrue(queue.Check());
        }

        //[Test]
        public void AllTest()
        {
            throw new NoSuchItemException();
        }

        [Test]
        [Repeat(10)]
        public void DeleteMinTest()
        {
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMin());
            if (prefill)
            {
                Random rng = new Random();
                for (int i = 0; i < n * threadCount; i++)
                    queue.Add(rng.Next(10000));
            }

            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                Thread t = new Thread(() =>
                {
                    Random random = new Random();
                    for (int y = 0; y < n; y++)
                        queue.DeleteMin();
                });
                threads[i] = t;
            }

            for (int i = 0; i < threads.Length; i++)
                threads[i].Start();
            for (int i = 0; i < threads.Length; i++)
                threads[i].Join();
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(0, queue.Count);
        }

        [Test]
        [Repeat(10)]
        public void DeleteMaxTest()
        {
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMax());
            if (prefill)
            {
                Random rng = new Random();
                for (int i = 0; i < n * threadCount; i++)
                    queue.Add(rng.Next(10000));
            }

            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                Thread t = new Thread(() =>
                {
                    Random random = new Random();
                    for (int y = 0; y < n; y++)
                        queue.DeleteMax();
                });
                threads[i] = t;
            }

            for (int i = 0; i < threads.Length; i++)
                threads[i].Start();
            for (int i = 0; i < threads.Length; i++)
                threads[i].Join();
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(0, queue.Count);
        }

        [Test]
        [Repeat(10)]
        public void ConcurrentTest()
        {
            int insertPercent = 50,
                deleteMinPercent = 25,
                deleteMax = 100 - insertPercent - deleteMinPercent;

            if (prefill)
            {
                Random rng = new Random();
                for (int i = 0; i < n; i++)
                    queue.Add(rng.Next(10000));
            }
            Thread[] threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                Thread t = new Thread(() =>
                {
                    Random random = new Random();
                    Random rng = new Random();
                    for (int y = 0; y < n; y++)
                    {
                        int percent = rng.Next(0, 100);
                        if (percent >= (100 - insertPercent))
                        {
                            queue.Add(random.Next(10000));
                        }
                        else if (percent >= (100 - insertPercent - deleteMinPercent))
                        {
                            try { queue.DeleteMin(); }
                            catch (NoSuchItemException e) {   /*ignore*/     }
                        }
                        else
                        {
                            try { queue.DeleteMax(); }
                            catch (NoSuchItemException e) {   /*ignore*/     }
                        }
                    }
                });
                threads[i] = t;
            }
            for (int i = 0; i < threads.Length; i++)
                threads[i].Start();
            for (int i = 0; i < threads.Length; i++)
                threads[i].Join();
            Assert.IsTrue(queue.Check());
        }
    }
    #endregion
}
