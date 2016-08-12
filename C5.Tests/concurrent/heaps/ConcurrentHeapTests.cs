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
using System.Collections.Generic;
using NUnit.Framework;
using System.Threading;
using System.Threading.Tasks;

namespace C5UnitTests.concurrent.heaps
{
    using C5.concurrent;
    using CollectionOfInt = C5.concurrent.ConcurrentIntervalHeap<int>;
    [TestFixture]
    public class SequentialTests
    {
        IConcurrentPriorityQueue<int> queue;

        [SetUp]
        public void Init() { queue = new ConcurrentIntervalHeap<int>(); }

        [TearDown]
        public void Dispose() { queue = null; }

        /// <summary>
        /// Test if the empty queue throws NoSuchItemException
        /// </summary>
        [Test]
        public void EmptyQueueTest()
        {
            Assert.AreEqual(0, queue.Count);
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMax());
            Assert.Throws<NoSuchItemException>(() => queue.DeleteMin());
        }

        /// <summary>
        /// Test if queue preseves correct structure while adding new elements
        /// </summary>
        [Test]
        public void AddElementsTest()
        {
            queue.Add(20);
            Assert.IsTrue(queue.Check());
            queue.Add(1);
            Assert.IsTrue(queue.Check());
            queue.Add(19);
            Assert.IsTrue(queue.Check());
            queue.Add(100);
            Assert.IsTrue(queue.Check());
        }

        /// <summary>
        /// Test if queue preserves correct structure while adding elements and 
        /// it returns expected Max element.
        /// </summary>
        [Test]
        public void AddRemoveMaxTest()
        {
            queue.Add(20);
            Assert.IsTrue(queue.Check());
            queue.Add(1);
            Assert.IsTrue(queue.Check());
            queue.Add(19);
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(20, queue.DeleteMax());
        }

        /// <summary>
        /// Test if queue preserves correct structure while adding elements and 
        /// it returns expected Min element.
        /// </summary>
        [Test]
        public void AddRemoveMinTest()
        {
            queue.Add(20);
            Assert.IsTrue(queue.Check());
            queue.Add(1);
            Assert.IsTrue(queue.Check());
            queue.Add(19);
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(1, queue.DeleteMin());
        }

        /// <summary>
        /// Test if queue returns expected queue size
        /// </summary>
        [Test]
        public void SizeTest()
        {
            queue.Add(20);
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(1, queue.Count);
            queue.Add(1);
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(2, queue.Count);
            queue.Add(19);
            Assert.IsTrue(queue.Check());
            Assert.AreEqual(3, queue.Count);
        }
    }




    /// <summary>
    /// Concurrent tests that should cover Add, RemoveMax, RemoveMin 
    /// operation by several threads.
    /// </summary>
    [TestFixture]
    class ConcurrencyTest
    {
        IConcurrentPriorityQueue<int> queue;
        int[] stack;

        [SetUp]
        public void Init() {
            queue = new C5.concurrent.ConcurrentIntervalHeap<int>();
            stack = new int[100];
            for(int i = 0; i<stack.Length; i++)
            {
                stack[i] = i;
            }

        }

        [TearDown]
        public void Dispose() {
            queue = null;
            stack = null;
        }

        [Test]
        public void ConcurrentAddTest()
        {
            Thread t1 = new Thread(() =>
            {
                for(int i = 0; i<stack.Length; i++)
                {
                    if (i % 2 == 0)
                        queue.Add(stack[i]);
                }
                
            });

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

            Assert.IsTrue(queue.Check());

            Assert.AreEqual(stack.Length, queue.Count);
            for(int i = 0; i < stack.Length; i++)
            {
                Assert.AreEqual(stack[i], queue.DeleteMin());
            }
            Assert.AreEqual(0, queue.Count);
        }

        

        /// <summary>
        /// Scalability test.
        /// run test on concurent interval heap with 4 threads
        /// and perform 1000 random operation.
        /// </summary>
        [Test]
        public void RandomOperationTest()
        {
            Random rnd = new Random();
            List<int> runs = new List<int>(4); //dummy list. 

            Parallel.ForEach(runs, r =>
            {
                for (int i = 0; i < 1000; i++)
                {
                    int random = rnd.Next(1, 3);

                    switch (random)
                    {
                        case 1:
                            //add operation
                            break;
                        case 2:
                            //remove max operation
                            break;
                        case 3:
                            //remove min operation;
                            break;
                    }
                }
            });
        }
    }
}
