using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;

namespace C5.concurrent
{
    public class RelaxedLotanShavitSkipList<T> : IConcurrentPriorityQueue<T>
    {
        /// <summary>
        /// works, but is slow
        /// </summary>
        public class Node
        {
            //An array the size of the "height" of the node, holds pointers to nodes on the seperate levels. 
            public Node[] forward;
            public int nodeLevel;
            public T value;
            public int deleted;
            public object nodeLock;
            public object[] levelLock;
            public bool tail;
            public int pid;

            public Node(int level, T newValue, bool tail = false)
            {
                forward = new Node[level];
                value = newValue;
                this.tail = tail;
                levelLock = new object[level];
                for (int i = 0; i < level; i++)
                {
                    levelLock[i] = new object();
                }
                nodeLevel = forward.Length;
                deleted = 0;
                nodeLock = new object();
                pid = 0;
            }

            public override string ToString()
            {
                return string.Format("{0}, {1}, {2}", value, tail, pid);
            }

        }

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        int size, maxLevel;
        int level;
        Node header, tail;
        Random random;
        long timeToMark, timeToSearch, timeToLock;

        public override string ToString()
        {
            return string.Format("Time to mark the nodes: {0}, Time to fill update array: {1}, Time to reassign references: {2}", timeToMark / 1000, timeToSearch / 1000, timeToLock / 1000);
        }
        public RelaxedLotanShavitSkipList() : this(32) { }

        public RelaxedLotanShavitSkipList(int max)
        {
            comparer = SCG.Comparer<T>.Default;
            itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            maxLevel = max;
            size = 0;
            level = 1;
            header = new Node(maxLevel, default(T));
            //so the header dosen't get picked up and marked by the delete operations
            header.deleted = 2;
            tail = new Node(0, default(T), true);
            tail.deleted = 2;
            for (int i = 0; i < maxLevel; i++)
            {
                header.forward[i] = tail;
            }

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
            lock (newNode.nodeLock)
            {
                for (int i = 0; i < newLevel; i++)
                {
                    try
                    {
                        node1 = getLock(update[i], item, i, ref lockTaken);

                        newNode.forward[i] = node1.forward[i];
                        node1.forward[i] = newNode;
                    }
                    finally
                    {
                        if (lockTaken)
                        {
                            Monitor.Exit(node1.levelLock[i]);
                            lockTaken = false;
                        }
                    }
                    
                }
            }
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
            int marked = -1;
            Node node1 = header;
            Node node2;
            Node[] update = new Node[maxLevel];
            Node[] update2 = new Node[maxLevel];
            bool lockTaken = false;

            while (true)
            {

                node1 = header;
                for (int i = maxLevel - 1; i >= 0; i--)
                {
                    while (!(node2 = node1.forward[i]).tail && node2.deleted != 1 && !node2.Equals(header))
                    {
                        node1 = node2;
                    }
                }


                if ((Interlocked.Exchange(ref node1.deleted, 1)) == 0)
                {
                    //Interlocked.Exchange(ref node1.pid, Thread.CurrentThread.ManagedThreadId);
                    break;
                }


            }
            //var stop = (DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond) - start;
            //Interlocked.Add(ref timeToMark, stop);
            retval = node1.value;
            retNode = node1;
            // start = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            node1 = header;
            for (int i = maxLevel - 1; i >= 0; i--)
            {

                node2 = node1.forward[i];
                while (!node2.tail && comparer.Compare(node2.value, retval) < 0)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                update[i] = node1;
            }
            //local search for a specific element
            for (int i = update.Length - 1; i >= 0; i--)
            {
                node2 = update[i];
                while (!node2.tail && node2 != retNode)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                update2[i] = node1;
            }

            node2 = retNode;


            lock (node2.nodeLock)
            {
                for (int i = node2.forward.Length - 1; i >= 0; i--)
                {
                    try
                    {
                        node1 = getMaxLock(update[i], retval, i, ref lockTaken, node2);
                        lock (node2.levelLock[i])
                        {
                            node1.forward[i] = node2.forward[i];
                            node2.forward[i] = node1;
                        }
                    }
                    finally
                    {
                        if (lockTaken)
                        {
                            Monitor.Exit(node1.levelLock[i]);
                            lockTaken = false;
                        }
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
            Node node1;
            Node node2;
            Node[] update = new Node[maxLevel];
            bool lockTaken = false;

            while (true)
            {
                node1 = header.forward[0];
                while (!node1.tail && !node1.Equals(header))
                {

                    if ((Interlocked.Exchange(ref node1.deleted, 1)) == 0)
                    {
                        //Interlocked.Exchange(ref node1.pid, Thread.CurrentThread.ManagedThreadId);
                        break;
                    }
                    node1 = node1.forward[0];
                }
                if (!node1.Equals(header))
                {
                    break;
                }
            }



            //SAVE the value of the node that we just marked for deletion
            retval = node1.value;
            Node temp = node1;
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
            node2 = node1;
            //make sure we have a pointer to the right node
            while (!node2.Equals(temp))
            {
                node2 = node2.forward[0];
            }

            lock (node2.nodeLock)
            {
                for (int i = node2.forward.Length - 1; i >= 0; i--)
                {
                    node1 = getMinLock(update[i], retval, i, ref lockTaken, temp);

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

            Node node1 = header;
            Node node2 = null;
            for (int i = maxLevel - 1; i >= 0; i--)
            {
                while (!(node2 = node1.forward[i]).tail && !node2.forward[i].tail)
                    node1 = node2;
            }
            return node1.forward[0].value;
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
                node1 = node2;
                Monitor.Enter(node1.levelLock[lvl], ref firstlocktaken);
                node2 = node1.forward[lvl];
            }

            return node1;
        }

        private Node getMaxLock(Node node1, T value, int lvl, ref bool firstlocktaken, Node retNode)
        {
            Node node2 = node1.forward[lvl];
            while (!node2.tail && node2 != retNode)
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
            while (node2.tail != true && node2 != retNode)
            {
                if (firstlocktaken)
                {
                    Monitor.Exit(node1.levelLock[lvl]);
                    firstlocktaken = false;
                }
                node1 = node2;
                Monitor.Enter(node1.levelLock[lvl], ref firstlocktaken);
                node2 = node1.forward[lvl];
            }

            return node1;
        }

        private Node getMinLock(Node node1, T value, int lvl, ref bool firstlocktaken, Node retNode)
        {
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
                node1 = node2;
                Monitor.Enter(node1.levelLock[lvl], ref firstlocktaken);
                node2 = node1.forward[lvl];
            }

            return node1;
        }



        private int RandomLevel()
        {
            var rng = new Random(BitConverter.ToInt32(Guid.NewGuid().ToByteArray(), 0));
            var lvl = 1;
            while (rng.Next(2) == 1 && lvl < maxLevel)
            {
                lvl++;
            }
            return lvl;
        }
    }
    #endregion
}
