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

namespace C5UnitTests.concurrent
{
    #region Sequential Tests
    [TestFixture]
    public class SequentialTests
    {
        IConcurrentPriorityQueue<int> queue;

        [SetUp]
        public void Init() { queue = new GlobalLockDEPQ<int>(); }

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
            Assert.IsTrue(queue.Check());
            queue.Add(1);
            Assert.IsTrue(queue.Check());
            queue.Add(19);
            Assert.IsTrue(queue.Check());
            queue.Add(100);
            Assert.IsTrue(queue.Check());
            queue.Add(0);
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(5, queue.Count);
        }

        [Test]
        public void RemoveMaxTest()
        {
            queue.Add(20);
            Assert.IsTrue(queue.Check());
            queue.Add(1);
            Assert.IsTrue(queue.Check());
            queue.Add(19);
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(20, queue.DeleteMax());
            Assert.AreEqual(19, queue.DeleteMax());
            Assert.AreEqual(1, queue.DeleteMax());
        }

        [Test]
        public void RemoveMinTest()
        {
            queue.Add(20);
            Assert.IsTrue(queue.Check());
            queue.Add(1);
            Assert.IsTrue(queue.Check());
            queue.Add(19);
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(1, queue.DeleteMin());
            Assert.AreEqual(19, queue.DeleteMax());
            Assert.AreEqual(20, queue.DeleteMax());
        }

        [Test]
        public void AllTest()
        {
            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.All());

            int[] elements = new int[] { 1, 25, 7, 80, 32 };
            foreach (int e in elements) { queue.Add(e); }
            int[] testElements = (int[])queue.All();
            Assert.AreEqual(elements.Length, testElements.Length);
            foreach (int e in elements)
            {
                int pos = Array.IndexOf(testElements, elements);
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

        /// <summary>
        /// Test setup.
        /// Enviroment.ProcessorCount is number of logical processors
        /// </summary>
        [SetUp]

        public void Init()
        {
            queue = new GlobalLockDEPQ<int>();
            threadCount = Environment.ProcessorCount + 2;
            n = 200;
        }

        [TearDown]
        public void Dispose()
        {
            queue = null;
        }


        [Test]
        [Repeat(10)]
        public void CountTest()
        {
            Assert.AreEqual(0, queue.Count);

            List<int> list = new List<int>();
            for (int i = 0; i < threadCount * 200; i++)
            {
                list.Add(new Random().Next(10000));
            }

            List<Thread> threads = new List<Thread>(threadCount);
            for (int i = 0; i < threads.Count; i++)
            {
                Thread t = new Thread(() =>
                {
                    for (int y = i * 200; y < (i + 1) * 200; y++)
                    {
                        queue.Add(list[y]);
                    }
                });
                threads.Add(t);
            }

            foreach (Thread t in threads)
            {
                t.Start();
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }
            Assert.AreEqual(list.Count, queue.Count);
        }

        [Test]
        [Repeat(10)]
        public void IsEmptyTest()
        {
            Assert.IsTrue(queue.IsEmpty());

            List<Thread> threads = new List<Thread>(threadCount);
            List<int> list = new List<int>();

            for (int i = 0; i < threadCount * 200; i++)
            {
                list.Add(new Random().Next(10000));
            }

            //Add elements to the queue            
            for (int i = 0; i < threads.Count; i++)
            {
                Thread t = new Thread(() =>
                {
                    for (int y = i * 200; y < (i + 1) * 200; y++)
                    {
                        queue.Add(list[y]);
                    }
                });
                threads.Add(t);
            }

            foreach (Thread t in threads)
            {
                t.Start();
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }

            Assert.IsFalse(queue.IsEmpty());

            //delete elements from the queue
            for (int i = 0; i < threads.Count; i++)
            {
                Thread t = new Thread(() =>
                {
                    for (int y = i * 200; y < (i + 1) * 200; y++)
                    {
                        queue.DeleteMax();
                    }
                });
                threads.Add(t);
            }

            foreach (Thread t in threads)
            {
                t.Start();
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }

            Assert.IsTrue(queue.IsEmpty());
        }

        [Test]
        [Repeat(10)]
        public void FindMaxTest()
        {
            Assert.Throws<NoSuchItemException>(() => queue.FindMax());

            List<Thread> threads = new List<Thread>(threadCount);
            List<int> list = new List<int>();

            for (int i = 0; i < threadCount * 200; i++)
            {
                list.Add(new Random().Next(10000));
            }

            //Add elements to the queue            
            for (int i = 0; i < threads.Count; i++)
            {
                Thread t = new Thread(() =>
                {
                    for (int y = i * 200; y < (i + 1) * 200; y++)
                    {
                        queue.Add(list[y]);
                    }
                });
                threads.Add(t);
            }

            foreach (Thread t in threads)
            {
                t.Start();
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }
            list.Sort();
            Assert.AreEqual(list[list.Count - 1], queue.FindMax());
        }

        [Test]
        [Repeat(10)]
        public void FindMinTest()
        {
            Assert.Throws<NoSuchItemException>(() => queue.FindMin());

            List<Thread> threads = new List<Thread>(threadCount);
            List<int> list = new List<int>();

            for (int i = 0; i < threadCount * 200; i++)
            {
                list.Add(new Random().Next(10000));
            }

            //Add elements to the queue            
            for (int i = 0; i < threads.Count; i++)
            {
                Thread t = new Thread(() =>
                {
                    for (int y = i * 200; y < (i + 1) * 200; y++)
                    {
                        queue.Add(list[y]);
                    }
                });
                threads.Add(t);
            }

            foreach (Thread t in threads)
            {
                t.Start();
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }
            list.Sort();
            Assert.AreEqual(list[0], queue.FindMin());
        }


        [Test]
        [Repeat(10)]
        public void AddTest()
        {
            Thread[] threads = new Thread[threadCount];
            Assert.AreEqual(threads.Length, threadCount);

            List<int> list = new List<int>();
            for (int i = 0; i < n; i++)
            {
                list.Add(new Random().Next(10000));
            }

            //adds numbers to the queue
            for (int i = 0; i < threadCount; i++)
            {
                threads[i] = new Thread(() =>
                {
                    for (int j = i * n; j < (j + 1) * n; j++)
                    {
                        queue.Add(list[i]);
                    }
                });
            }

            for (int i = 0; i < threadCount; i++) { threads[i].Start(); }
            try { for (int i = 0; i < threadCount; i++) threads[i].Join(); }
            catch (ThreadInterruptedException exn) { }

            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);

            List<int> listTest = new List<int>();
            for (int i = 0; i < list.Count; i++)
            {
                listTest.Add(queue.DeleteMax());
            }

            Assert.AreEqual(list.Count, listTest.Count);

            for (int i = 0; i < list.Count; i++)
            {
                Assert.IsTrue(listTest.Contains(list[i]));
            }
        }

        [Test]
        [Repeat(10)]
        public void AllTest()
        {
            
            Thread[] threads = new Thread[threadCount];
            Assert.AreEqual(threads.Length, threadCount);

            List<int> list = new List<int>();
            for (int i = 0; i < n * threadCount; i++)
            {
                list.Add(new Random().Next(10000));
            }

            //adds numbers to the queue
            for (int i = 0; i < threadCount; i++)
            {
                threads[i] = new Thread(() =>
                {
                    for (int j = i * n; j < (j + 1) * n; j++)
                    {
                        queue.Add(list[i]);
                    }
                });
            }

            for (int i = 0; i < threadCount; i++) { threads[i].Start(); }
            try { for (int i = 0; i < threadCount; i++) threads[i].Join(); }
            catch (ThreadInterruptedException exn) { }


            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);

            List<int> listTest = new List<int>();
            listTest = (List<int>)queue.All();

            Assert.AreEqual(list.Count, listTest.Count);
            for (int i = 0; i < list.Count; i++)
            {
                Assert.IsTrue(listTest.Contains(list[i]));
            }
        }

        [Test]
        [Repeat(10)]
        public void DelteMinTest()
        {
            Thread[] threads = new Thread[threadCount];
            Assert.AreEqual(threads.Length, threadCount);

            List<int> list = new List<int>();
            for (int i = 0; i < n; i++)
            {
                list.Add(new Random().Next(10000));
            }
            list.Sort();

            //adds numbers to the queue
            for (int i = 0; i < threadCount; i++)
            {
                threads[i] = new Thread(() =>
                {
                    for (int j = i * n; j < (j + 1) * n; j++)
                    {
                        queue.Add(list[i]);
                    }
                });
            }

            for (int i = 0; i < threadCount; i++) { threads[i].Start(); }
            try { for (int i = 0; i < threadCount; i++) threads[i].Join(); }
            catch (ThreadInterruptedException exn) { }

            //check if queue has correct structure.
            Assert.AreEqual(list.Count, queue.Count);

            for (int i = list.Count - 1; i >= 0; i--)
            {
                Assert.AreEqual(list[i], queue.DeleteMin());
            }

            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMin());

        }

        [Test]
        [Repeat(10)]
        public void DeleteMaxTest()
        {
            Thread[] threads = new Thread[threadCount];
            Assert.AreEqual(threads.Length, threadCount);

            List<int> list = new List<int>();
            for (int i = 0; i < n; i++)

            {
                list.Add(new Random().Next(10000));
            }
            list.Sort();
            //adds numbers to the queue

            for (int i = 0; i < threadCount; i++)
            {
                threads[i] = new Thread(() =>
                {
                    for (int j = i * n; j < (j + 1) * n; j++)
                    {
                        queue.Add(list[i]);
                    }
                });
            }

            for (int i = 0; i < threadCount; i++) { threads[i].Start(); }
            try { for (int i = 0; i < threadCount; i++) threads[i].Join(); }
            catch (ThreadInterruptedException exn) { }

            //check if queue has correct structure.
            Assert.AreEqual(list.Count, queue.Count);

            for (int i = list.Count - 1; i >= 0; i--)
            {
                Assert.AreEqual(list[i], queue.DeleteMax());
            }

            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMax());
        }
    }
    #endregion
}
