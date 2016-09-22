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
using System.Collections.Generic;
using NUnit.Framework;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace C5UnitTests.concurrent
{
    using C5.concurrent;

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

    /// <summary>
    /// Concurrent tests that should cover Add, RemoveMax, RemoveMin 
    /// operation by several threads.
    /// </summary>
    [TestFixture]
    class ConcurrencyTest
    {
        private IConcurrentPriorityQueue<int> queue;
        private int threadCount;

        [SetUp]
        public void Init()
        {
            queue = new GlobalLockDEPQ<int>();
            //Enviroment.ProcessorCount is number of logical processors
            threadCount = Environment.ProcessorCount + 2;

        }

        [TearDown]
        public void Dispose()
        {
            queue = null;
        }


        [Test]
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
        public void AddTest()
        {
            int[] stack = new int[100];
            for (int i = 0; i < stack.Length; i++)
            {
                stack[i] = new Random().Next(0, 1000);
            }
            Array.Sort(stack);

            //define thread work: add to the queue even numbers. 


            Thread t1 = new Thread(() =>
            {
                for (int i = 0; i < stack.Length; i++)
                {
                    if (i % 2 == 0)
                        queue.Add(stack[i]);
                }
            });

            //define thread work: add to the queue odd numbers.
            Thread t2 = new Thread(() =>
            {
                for (int i = 0; i < stack.Length; i++)
                {
                    if (i % 2 != 0)
                        queue.Add(stack[i]);
                }
            });

            t1.Start();
            t2.Start();

            t1.Join();
            t2.Join();

            Assert.IsTrue(queue.Check()); //check if queue has correct tructure.
            Assert.AreEqual(stack.Length, queue.Count);

            //check if elements are deleted in order. 
            for (int i = 0; i < stack.Length; i++)
            {
                Assert.AreEqual(stack[i], queue.DeleteMin());
            }
            Assert.AreEqual(0, queue.Count);
        }



        [Test]
        public void RandomOperation()
        {
            Thread[] threads = new Thread[4];

            for (int i = 0; i < threads.Length; i++)
            {
                Thread t = new Thread(() =>
                {
                    int iterations = 0;
                    while (iterations < 1000)
                    {
                        int randomOp = new Random().Next(0, 100);
                        if (randomOp <= 15)
                        {
                            queue.DeleteMin();
                        }
                        else if (randomOp <= 35)
                        {
                            queue.DeleteMax();
                        }
                        else if (randomOp <= 50)
                        {
                            int randomInt = new Random().Next(0, 1000);
                            queue.Add(randomInt);
                        }

                        iterations++;
                    }
                });

                threads[i] = t;
                //no element is lost 
                //no element is duplicated. make each
            }

            for (int i = 0; i < threads.Length; i++)
            {
                threads[i].Start();
            }

            for (int i = 0; i < threads.Length; i++)
            {
                threads[i].Join();
            }

            Assert.IsTrue(queue.Check());

            int greather = int.MaxValue;
            while (queue.Count > 500)
            {
                int current = queue.DeleteMax();
                Assert.IsTrue(greather >= current);
                greather = current;
            }

            int lesser = int.MinValue;
            while (queue.Count > 0)
            {
                int current = queue.DeleteMin();
                Assert.IsTrue(lesser <= current);
                lesser = current;
            }

        }

        [Test]
        public void DelteMinTest()
        {


            int[] stack = new int[100];
            for (int i = 0; i < stack.Length; i++)
            {
                stack[i] = new Random().Next(0, 1000);
            }
            Array.Sort(stack);

        }

        public void DeleteMaxTest()
        {

        }
    }
}
