﻿using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace C5.concurrent
{
    public class LotanShavitSkiplist<T> : IConcurrentPriorityQueue<T>
    {
        public class Node
        {
            //An array the size of the "height" of the node, holds pointers to nodes on the seperate levels. 
            public Node[] forward;
            public int nodeLevel;
            public T value;
            public long timeStamp;
            public int deleted;
            public object nodeLock;
            public object[] levelLock;
            public int[] levelTag;
            public bool tail;
            public int pid;

            public Node(int level, T newValue, bool tail = false)
            {
                forward = new Node[level];
                value = newValue;
                this.tail = tail;
                levelLock = new object[level];
                levelTag = new int[level];
                for (int i = 0; i < level; i++)
                {
                    levelLock[i] = new object();
                }
                nodeLevel = forward.Length;
                timeStamp = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                deleted = 0;
                nodeLock = new object();
                pid = 0;
            }

            public override string ToString()
            {
                return string.Format("{0}, {1}", value, tail);
            }

        }

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        int size, maxLevel;
        int level;
        Node header, tail;
        Random random;
        public LotanShavitSkiplist() : this(32) { }

        public LotanShavitSkiplist(int max)
        {
            comparer = SCG.Comparer<T>.Default;
            itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            maxLevel = max;
            size = 0;
            level = 1;
            header = new Node(maxLevel, default(T));
            //so the header dosen't get picked up and marked by the delete operations
            header.timeStamp = DateTime.MaxValue.Ticks / TimeSpan.TicksPerMillisecond;
            tail = new Node(0, default(T), true);
            tail.timeStamp = DateTime.MaxValue.Ticks / TimeSpan.TicksPerMillisecond;
            for (int i = 0; i < maxLevel; i++)
            {
                header.forward[i] = tail;
            }
            random = new Random();
        }

        public int Count
        {
            get { return size; }
        }

        public bool Add(T item)
        {
            if (add(item))
                return true;
            return false;
        }

        private bool add(T item)
        {
            Node[] update = new Node[maxLevel];
            Node node1 = header;
            Node node2;
            Node newNode;
            bool lockTaken = false;

            for (int i = maxLevel - 1; i >= 0; i--)
            {
                node2 = node1.forward[i];
                while (node2.tail != true && comparer.Compare(node2.value, item) < 0)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                update[i] = node1;
            }

            int newLevel = RandomLevel();
            newNode = new Node(newLevel, item);
            newNode.timeStamp = DateTime.MaxValue.Ticks / TimeSpan.TicksPerMillisecond;
            lock (newNode.nodeLock)
            {
                for (int i = 0; i < newLevel; i++)
                {
                    node1 = getLock(update[i], item, i, ref lockTaken);

                    newNode.forward[i] = node1.forward[i];
                    node1.forward[i] = newNode;

                    if (lockTaken)
                    {
                        Monitor.Exit(node1.levelLock[i]);
                        lockTaken = false;
                    }

                }
            }
            newNode.timeStamp = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            Interlocked.Increment(ref size);
            return true;
        }


        public SCG.IEnumerable<T> All()
        {
            Node x = header;
            if (x.forward[0].tail)
                throw new NoSuchItemException();


            T[] elements = new T[size];
            int i = 0;
            while (!x.forward[0].tail)
            {
                elements[i] = x.forward[0].value;
                x = x.forward[0];
                i++;
            }
            return elements;
        }

        /// <summary>
        ///  Checks if all the elements in the list are in order, but does not check the pointers on the levels. 
        /// </summary>
        /// <returns>bool</returns>
        public bool Check()
        {
            //no elements, so the list is in order.
            if (size < 2)
                return true;

            //check the queue have the correct number of elements
            Node x = header;
            int j = 0;
            while (x.forward[0].tail != true && j < size)
            {
                x = x.forward[0];
                j++;
            }
            if (j != size)
            {
                throw new Exception("number of elements differ from size: " + "j: " + j + " size: " + size);
                //return false;
            }
            x = header;
            //If the next node is not null and the current node's value is less then or equel to (equel to as we allow for duplicates as of 21/11/2016)
            //we continue to move forward at the bottom level of the list. 
            for (int i = 0; i < x.forward.Length; i++)
            {
                while (x.forward[i].tail != true && comparer.Compare(x.value, x.forward[i].value) <= 0)
                    x = x.forward[i];

                //if the foward pointers of the last element of this level does not point to the tail
                //then something is wrong
                if (!x.forward[i].tail)
                    throw new Exception("next element is not tail: " + x);
                //return false;
            }

            return true;
        }

        public T DeleteMax()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();
            Node retNode;
            T retval;
            int marked = 2;
            Node node1 = header;
            Node node2;
            Node[] update = new Node[maxLevel];
            bool lockTaken = false;
            long searchTimestamp = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            Node temp;

            //Initial search, in the paper Lotan / shavit traverse the bottom layer to check ALL nodes for the first one that
            //was inserted before the search began, skipping all that was inserted after.
            // when deleting from the max end of the queue, the issue becomes that using this approach means that the search time
            //    will effictivly be in liniar o(n) instead of log n
            //our suggested approach uses the traditional skip search and then checks for the time stamp, combining the best of both methods
            while (true)
            {
                node1 = header;
                for (int i = maxLevel - 1; i >= 0; i--)
                {
                    while (!(node2 = node1.forward[i]).tail)
                    {
                        node1 = node2;
                    }
                }

                if (node1.timeStamp <= searchTimestamp && node1.pid != Thread.CurrentThread.ManagedThreadId)
                {
                    marked = Interlocked.Exchange(ref node1.deleted, 1);
                    if (marked == 0)
                    {
                        break;
                    }
                }

            }
            node1.pid = Thread.CurrentThread.ManagedThreadId;
            retval = node1.value;
            retNode = node1;

            for (int i = maxLevel - 1; i >= 0; i--)
            {
                node1 = header;
                node2 = node1.forward[i];
                while (!node2.tail && node2 != retNode)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                update[i] = node1;
            }
            
            node2 = node1.forward[0];
            while (node2 != retNode)
            {
                node2 = node2.forward[0];
            }

            lock (node2.nodeLock)
            {
                retval = node2.value;
                for (int i = node2.forward.Length - 1; i >= 0; i--)
                {
                    node1 = getLock(update[i], retval, i, ref lockTaken);
                    lock (node2.levelLock[i])
                    {
                        node1.forward[i] = node2.forward[i];
                        node2.forward[i] = node1;
                    }
                    if (lockTaken)
                    {
                        Monitor.Exit(node1.levelLock[i]);
                        lockTaken = false;
                    }


                }
            }
            Interlocked.Decrement(ref size);
            return retval;
        }

        public T DeleteMin()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();

            T retval;
            int marked = -1;
            //taking the first element so the header will NOT be marked for deletion
            Node node1;
            Node node2;
            Node[] update = new Node[maxLevel];
            bool lockTaken = false;
            var time = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;


            node1 = header.forward[0];
            //search until unmarked (ei. 0) node found, and while the node is not currently being deleted
            while (!node1.tail)
            {
                //only take the nodes that was inserted before the search began
                if (node1.timeStamp <= time)
                {
                    marked = Interlocked.Exchange(ref node1.deleted, 1);
                }
                if (marked == 0)
                {
                    Interlocked.Exchange(ref node1.pid, Thread.CurrentThread.ManagedThreadId);
                    break;
                }
                node1 = node1.forward[0];

            }
            
            if (node1 == header)
            {
                throw new Exception("node1 is header, the header should never be marked for deletion");
            }
            if (node1.pid != Thread.CurrentThread.ManagedThreadId)
            {
                throw new Exception("FUCK pid was not the same");
            }


            //SAVE the value of the node that we just marked for deletion
            //########################

            retval = node1.value;
            Node retNode = node1;
            //#######################
            node1 = header;
            for (int i = maxLevel - 1; i >= 0; i--)
            {
                node2 = node1.forward[i];
                while (!node2.tail && comparer.Compare(node2.value, retval) < 0)
                {
                    node1 = node2;
                    node2 = node1.forward[i];
                }
                //while (!node2.isTail && node2 )
                //{
                //    node1 = node2;
                //    node2 = node1.forward[i];
                //}
                update[i] = node1;
            }
            if (comparer.Compare(node1.value, retval) > 0)
            {
                throw new Exception(node1 + " " + retval);
            }
            node2 = node1.forward[0];
            //make sure we have a pointer to the right node
            while (node2 != retNode)
            {
                node2 = node2.forward[0];
            }
            
            lock (node2.nodeLock)
            {
                for (int i = node2.forward.Length - 1; i >= 0; i--)
                {
                    node1 = getMinLock(update[i], retval, i, ref lockTaken, retNode);
                    if(node1.forward[i] != retNode)
                    {
                        throw new Exception("wrong predecessor " + node1 + " " + node2);
                    }
                    lock (node2.levelLock[i])
                    {
                        node1.forward[i] = node2.forward[i];
                        node2.forward[i] = node1;
                    }
                    if (lockTaken)
                    {
                        Monitor.Exit(node1.levelLock[i]);
                        lockTaken = false;
                    }
                }
            }
            Interlocked.Decrement(ref size);
            return retval;
        }

        public T FindMax()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();

            Node x = header;
            for (int i = maxLevel; i >= 0; i--)
            {
                while (!x.forward[i].tail && !x.forward[i].forward[i].tail)
                    x = x.forward[i];
            }
            return x.forward[0].value;
        }

        public T FindMin()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();
            return header.forward[0].value;
        }

        public bool IsEmpty()
        {
            if (header.forward[0].tail)
                return true;
            return false;
        }

        #region helpers


        private Node[] doSearch(Node head, T item, Node[] update)
        {
            Node x;
            for (int i = head.forward.Length - 1; i >= 0; i--)
            {
                x = head.forward[i];
                while (x.tail != true && comparer.Compare(x.value, item) <= 0)
                {
                    head = x;
                    x = x.forward[i];
                }
                update[i] = head;
            }
            return update;
        }

        private Node[] maxSearch(Node head, Node[] update)
        {
            for (int i = head.forward.Length - 1; i >= 0; i--)
            {
                while (head.forward[i].tail != true && comparer.Compare(head.forward[i].value, head.value) >= 0)
                {
                    head = head.forward[i];
                }
                update[i] = head;
            }
            return update;
        }
        //search for the max node
        private Node maxNodeSearch(Node head, long searchStartTime)
        {
            for (int i = head.forward.Length - 1; i >= 0; i--)
            {
                while (head.forward[i].tail != true && comparer.Compare(head.forward[i].value, head.value) >= 0)
                {
                    //optimisation idea: instead of traverisng all the levels, if the last level's forward pointer, points
                    // to the tail, then we know this is the last element and we will return that. HOWEVER this might be an issue
                    // combined with the timestamps, as, in a realistic exampel the last node will in fact not be the tail, but a node inserted
                    // after the search began. 
                    if (head.forward[0].tail == true || head.forward[0].timeStamp > searchStartTime)
                    {
                        return head;
                    }
                    head = head.forward[i];
                }
            }
            return head;
        }

        //fills the update array
        private Node[] doSearch(Node head, Node[] update)
        {
            for (int i = head.forward.Length - 1; i >= 0; i--)
            {
                while (head.forward[i].tail != true && comparer.Compare(head.forward[i].value, head.value) <= 0)
                {
                    head = head.forward[i];
                }
                update[i] = head;
            }
            return update;
        }

        private Node[] doSearch(Node head, Node findThis, Node[] update)
        {
            Node x = head;
            for (int i = x.forward.Length - 1; i >= 0; i--)
            {
                while (x.forward[i].tail != true && comparer.Compare(x.forward[i].value, findThis.value) <= 0 && comparer.Compare(x.forward[i].value, x.value) >= 0)
                {
                    head = x;
                    x = x.forward[i];
                }
                update[i] = head;
            }
            return update;
        }

        private Node getLock(Node node1, T value, int lvl, ref bool firstlocktaken)
        {
            Node node2 = node1.forward[lvl];
            while (node2.tail != true && comparer.Compare(node2.value, value) < 0)
            {
                node1 = node2;
                node2 = node2.forward[lvl];
            }
            try
            {
                Monitor.Enter(node1.levelLock[lvl], ref firstlocktaken);
            }
            catch (Exception e)
            {

                throw e;
            }
            //try to search again, check if something has changed 
            node2 = node1.forward[lvl]; //orginal psudocode
            while (node2.tail != true && comparer.Compare(node2.value, value) < 0)
            {
                if (firstlocktaken)
                {
                    Monitor.Exit(node1.levelLock[lvl]);
                    firstlocktaken = false;
                }
                //tempNode = node2;
                node1 = node2;
                Monitor.Enter(node1.levelLock[lvl], ref firstlocktaken);
                node2 = node1.forward[lvl];
            }

            return node1;
        }

        private Node getMinLock(Node node1, T value, int lvl, ref bool firstlocktaken, Node retNode)
        {
            Node paramNode = node1;
            Node node2 = node1.forward[lvl];
            while (node2.tail != true && comparer.Compare(node2.value, value) <= 0 && node2 != retNode)
            {
                node1 = node2;
                node2 = node2.forward[lvl];
            }
            try
            {
                Monitor.Enter(node1.levelLock[lvl], ref firstlocktaken);
            }
            catch (Exception e)
            {

                throw e;
            }
            //try to search again, check if something has changed 
            node2 = node1.forward[lvl]; //orginal psudocode
            while (node2.tail != true && comparer.Compare(node2.value, value) <= 0 && node2 != retNode)
            {
                if (firstlocktaken)
                {
                    Monitor.Exit(node1.levelLock[lvl]);
                    firstlocktaken = false;
                }
                //tempNode = node2;
                node1 = node2;
                Monitor.Enter(node1.levelLock[lvl], ref firstlocktaken);
                node2 = node1.forward[lvl];
            }

            return node1;
        }


        private Node[] searchAndLock(Node head, T item, Node[] update)
        {
            for (int i = head.forward.Length - 1; i >= 0; i--)
            {
                Node node2 = head;
                while (node2.forward[i] != null && comparer.Compare(node2.forward[i].value, item) <= 0)
                {
                    head = node2;
                    node2 = head.forward[i];
                }
                Monitor.Enter(head.levelLock[i]);
                update[i] = head;
            }
            return update;
        }



        private int ShavitRandomLevel()
        {
            var lvl = 1;
            while (random.Next(2) == 1)
            {
                lvl++;
            }
            if (lvl > maxLevel)
            {
                return maxLevel;
            }
            return lvl;
        }
        private int RandomLevel()
        {
            var lvl = 1;
            while (random.Next(2) == 1 && lvl < maxLevel)
            {
                lvl++;
            }
            return lvl;
        }
    }
    #endregion
}
