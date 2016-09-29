﻿/*
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
            Assert.AreEqual(19, queue.DeleteMin());
            Assert.AreEqual(20, queue.DeleteMin());
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

        /// <summary>
        /// Test setup.
        /// Enviroment.ProcessorCount is number of logical processors
        /// </summary>
        [SetUp]

        public void Init()
        {
            queue = new GlobalLockDEPQ<int>();
            threadCount = Environment.ProcessorCount + 2;
            n = 2;
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
            Random rng = new Random();
            List<int> list = new List<int>();
            for (int i = 0; i < n * threadCount; i++)
            {
                list.Add(rng.Next(1000));
            }

            List<int[]> partitions = new List<int[]>();
            for (int i = 0; i < threadCount; i++)
            {
                int[] par = new int[n];
                int c = 0;
                for (int j = i * n; j < ((i + 1) * n); j++)
                {
                    par[c] = list[j];
                    c++;
                }
                partitions.Add(par);
            }

            Parallel.ForEach(partitions, (partition) =>
            {
                for (int i = 0; i < partition.Length; i++)
                {
                    queue.Add(partition[i]);
                }
            });

            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);
            Parallel.ForEach(partitions, (partition) =>
            {
                int c = n;
                while (c > 0)
                {
                    queue.DeleteMin();
                    c--;
                }
            });
            Assert.AreEqual(0, queue.Count);
        }

        [Test]
        [Repeat(10)]
        public void IsEmptyTest()
        {
            Assert.IsTrue(queue.IsEmpty());
            Random rng = new Random();
            List<int> list = new List<int>();
            for (int i = 0; i < n * threadCount; i++)
            {
                list.Add(rng.Next(1000));
            }

            List<int[]> partitions = new List<int[]>();
            for (int i = 0; i < threadCount; i++)
            {
                int[] par = new int[n];
                int c = 0;
                for (int j = i * n; j < ((i + 1) * n); j++)
                {
                    par[c] = list[j];
                    c++;
                }
                partitions.Add(par);
            }

            Parallel.ForEach(partitions, (partition) =>
            {
                for (int i = 0; i < partition.Length; i++)
                {
                    queue.Add(partition[i]);
                }
            });

            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);

            Parallel.ForEach(partitions, (partition) =>
            {
                int c = n;
                while (c > 0)
                {
                    queue.DeleteMin();
                    c--;
                }
            });

            Assert.IsTrue(queue.IsEmpty());
        }

        [Test]
        [Repeat(10)]
        public void FindMaxTest()
        {
            Assert.Throws<NoSuchItemException>(() => queue.FindMax());
            Random rng = new Random();
            List<int> list = new List<int>();
            for (int i = 0; i < n * threadCount; i++)
            {
                list.Add(rng.Next(1000));
            }

            List<int[]> partitions = new List<int[]>();
            for (int i = 0; i < threadCount; i++)
            {
                int[] par = new int[n];
                int c = 0;
                for (int j = i * n; j < ((i + 1) * n); j++)
                {
                    par[c] = list[j];
                    c++;
                }
                partitions.Add(par);
            }

            Parallel.ForEach(partitions, (partition) =>
            {
                for (int i = 0; i < partition.Length; i++)
                {
                    queue.Add(partition[i]);
                }
            });

            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);
            list.Sort();
            Assert.AreEqual(list[list.Count - 1], queue.FindMax());
        }

        [Test]
        [Repeat(10)]
        public void FindMinTest()
        {
            Assert.Throws<NoSuchItemException>(() => queue.FindMin());
            Random rng = new Random();
            List<int> list = new List<int>();
            for (int i = 0; i < n * threadCount; i++)
            {
                list.Add(rng.Next(1000));
            }

            List<int[]> partitions = new List<int[]>();
            for (int i = 0; i < threadCount; i++)
            {
                int[] par = new int[n];
                int c = 0;
                for (int j = i * n; j < ((i + 1) * n); j++)
                {
                    par[c] = list[j];
                    c++;
                }
                partitions.Add(par);
            }

            Parallel.ForEach(partitions, (partition) =>
            {
                for (int i = 0; i < partition.Length; i++)
                {
                    queue.Add(partition[i]);
                }
            });

            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);
            list.Sort();
            Assert.AreEqual(list[0], queue.FindMin());
        }


        [Test]
        [Repeat(10)]
        public void AddTest()
        {
            Random rng = new Random();
            List<int> list = new List<int>();
            for (int i = 0; i < n * threadCount; i++)
            {
                list.Add(rng.Next(1000));
            }

            List<int[]> partitions = new List<int[]>();
            for (int i = 0; i < threadCount; i++)
            {
                int[] par = new int[n];
                int c = 0;
                for (int j = i * n; j < ((i + 1) * n); j++)
                {
                    par[c] = list[j];
                    c++;
                }
                partitions.Add(par);
            }

            Parallel.ForEach(partitions, (partition) =>
            {
                for (int i = 0; i < partition.Length; i++)
                {
                    queue.Add(partition[i]);
                }
            });

            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);

            List<int> listTest = new List<int>();
            while(queue.Count > 0)
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
            Assert.Throws<NoSuchItemException>(() => queue.All());
            Random rng = new Random();
            List<int> list = new List<int>();
            for (int i = 0; i < n * threadCount; i++)
            {
                list.Add(rng.Next(1000));
            }

            List<int[]> partitions = new List<int[]>();
            for (int i = 0; i < threadCount; i++)
            {
                int[] par = new int[n];
                int c = 0;
                for (int j = i * n; j < ((i + 1) * n); j++)
                {
                    par[c] = list[j];
                    c++;
                }
                partitions.Add(par);
            }

            Parallel.ForEach(partitions, (partition) =>
            {
                for (int i = 0; i < partition.Length; i++)
                {
                    queue.Add(partition[i]);
                }
            });

            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);

            List<int> listTest = new List<int>((int[])queue.All());
            Assert.AreEqual(list.Count, listTest.Count);
            for (int i = 0; i < list.Count; i++)
            {
                Assert.IsTrue(listTest.Contains(list[i]));
            }
        }

        [Test]
        [Repeat(10)]
        public void DeleteMinTest()
        {
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMin());
            Random rng = new Random();
            List<int> list = new List<int>();
            for (int i = 0; i < n * threadCount; i++)
            {
                list.Add(rng.Next(1000));
            }

            List<int[]> partitions = new List<int[]>();
            for (int i = 0; i < threadCount; i++)
            {
                int[] par = new int[n];
                int c = 0;
                for (int j = i * n; j < ((i + 1) * n); j++)
                {
                    par[c] = list[j];
                    c++;
                }
                partitions.Add(par);
            }

            Parallel.ForEach(partitions, (partition) =>
            {
                for (int i = 0; i < partition.Length; i++)
                {
                    queue.Add(partition[i]);
                }
            });

            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);

            list.Sort();
            for (int i = 0; i < list.Count; i++)
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
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMax());

            Random rng = new Random();
            List<int> list = new List<int>();
            for (int i = 0; i < n * threadCount; i++)
            {
                list.Add(rng.Next(1000));
            }

            List<int[]> partitions = new List<int[]>();
            for (int i = 0; i < threadCount; i++)
            {
                int[] par = new int[n];
                int c = 0;
                for (int j = i * n; j < ((i + 1) * n); j++)
                {
                    par[c] = list[j];
                    c++;
                }
                partitions.Add(par);
            }

            Parallel.ForEach(partitions, (partition) =>
            {
                for (int i = 0; i < partition.Length; i++)
                {
                    queue.Add(partition[i]);
                }
            });

            Assert.IsTrue(queue.Check());
            Assert.AreEqual(list.Count, queue.Count);

            list = list.OrderByDescending(i => i).ToList();
            for (int i = 0; i < list.Count; i++)
            {
                int o = list[i];
                int d = queue.FindMax();
                if (o != d)
                {
                    i = i;
                }
                d = queue.DeleteMax();
                Assert.AreEqual(o, d);
            }

            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMax());
        }
    }
    #endregion
}
