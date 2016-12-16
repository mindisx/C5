using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace C5.concurrent
{
    public class SkipQueue<T> : IConcurrentPriorityQueue<T>
    {
        public class Node
        {
            public Node[] forward;
            public int nodeLevel;
            public T value;
            public long timeStamp;
            public int deleted;
            public object nodeLock;
            public object[] levelLock;
            public int[] levelTag;
            public bool isTail;
            public int pid;

            public Node(int level, T newValue, bool tail = false)
            {
                forward = new Node[level];
                value = newValue;
                isTail = tail;
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
            }

            public override string ToString()
            {
                return string.Format("{0}, {1}", value, isTail);
            }

        }

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        int size, maxLevel;
        int level;
        Node header, tail;

        public SkipQueue() : this(32) { }

        public SkipQueue(int max)
        {
            comparer = SCG.Comparer<T>.Default;
            itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            maxLevel = max;
            size = 0;
            level = 1;
            header = new Node(maxLevel, default(T));
            header.timeStamp = DateTime.MaxValue.Ticks / TimeSpan.TicksPerMillisecond;
            tail = new Node(0, default(T), true);
            tail.timeStamp = DateTime.MaxValue.Ticks / TimeSpan.TicksPerMillisecond;
            for (int i = 0; i < maxLevel; i++)
            {
                header.forward[i] = tail;
            }
        }

        public int Count
        {
            get { return size; }
        }

        private bool lockAll(Node lockOnThis, Func<Boolean> action)
        {
            int s = lockOnThis.levelLock.Length;
            for (int i = 0; i < s; i++)
            {
                Monitor.Enter(lockOnThis.levelLock[i]);
            }
            try
            {
                return action();
            }
            finally
            {
                for (int i = s - 1; i >= 0; i--)
                {
                    Monitor.Exit(lockOnThis.levelLock[i]);
                }
            }

            //return lockAll(0, action);
        }




        public bool Add(T item)
        {
            if (add(item))
                return true;
            return false;
        }

        private bool add(T item)
        {
            Node[] savedNodes = new Node[maxLevel];
            Node node1 = header;
            Node node2;
            Node newNode;
            bool lockTaken = false;

            //fill update array with search, we note that this might not provide the same reult as Lotan Shavit,
            //As we have duplicates which will "trick" the search. 
            for (int i = maxLevel - 1; i >= 0; i--)
            {
                node2 = node1.forward[i];
                while (node2.isTail != true && comparer.Compare(node2.value, item) < 0)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                savedNodes[i] = node1;
            }


            //here, in the psudocode, a lock on the bottom level is obtained
            //and a check is performed to see that the insertion key is not the same as the current one
            //IF it is, the value of he key is updated

            int newLevel = RandomLevel();
            newNode = new Node(newLevel, item);
            newNode.timeStamp = DateTime.MaxValue.Ticks / TimeSpan.TicksPerMillisecond;
            lock (newNode.nodeLock)
            {
                for (int i = 0; i < newLevel; i++)
                {
                    node1 = getInsertionLock(savedNodes[i], item, i, ref lockTaken);

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
            if (x.forward[0].isTail)
                throw new NoSuchItemException();


            T[] elements = new T[size];
            int i = 0;
            while (!x.forward[0].isTail)
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
            while (x.forward[0].isTail != true && j < size)
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
                while (x.forward[i].isTail != true && comparer.Compare(x.value, x.forward[i].value) <= 0)
                    x = x.forward[i];

                //if the foward pointers of the last element of this level does not point to the tail
                //then something is wrong
                if (!x.forward[i].isTail)
                    throw new Exception("next element is not tail" + "x: " + x + " i: " + i + " x.forward: " + x.forward[i] + " x.forward.forward: " + x.forward[i].forward[i]);
                //return false;
            }
            x = header;
            //check whether all elements that where supposed to be deleted, where indded deleted
            while (!x.forward[0].isTail)
            {
                if (x.deleted != 0)
                    throw new Exception("element x is marked, but not removed: " + "x: " + x + " x.forward: " + x.forward[0] + " x.forward.forward: " + x.forward[0].forward[0]);
                x = x.forward[0];
            }

            return true;
        }

        public T DeleteMax()
        {
            if (header.forward[0].isTail)
                throw new NoSuchItemException();

            T retval;
            int marked = -1;
            //taking the first element so the header will NOT be marked for deletion
            Node node1 = header;
            Node node2;
            Node[] update = new Node[maxLevel];
            bool lockTaken = false;
            long searchTimestamp = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
            var retry = false;
           
            //our suggested approach uses the traditional skip search and then checks for the time stamp, combining the best of both methods
            while (marked != 0)
            {
                node1 = header;
                for (int i = maxLevel - 1; i >= 0; i--)
                {
                    while (node1.forward[i].isTail != true && comparer.Compare(node1.forward[i].value, node1.value) >= 0 && node1.forward[i].deleted != 1)
                    {
                       
                        node1 = node1.forward[i];
                    }
                }

                if (node1.timeStamp <= searchTimestamp)
                {
                    marked = Interlocked.Exchange(ref node1.deleted, 1);
                    if (marked == 0)
                    {
                        break;
                    }
                }

            }

           
            //SAVE the value of the node that we just marked for deletion
            retval = node1.value;
            Node retNode = node1;

            for (int i = maxLevel - 1; i >= 0; i--)
            {
                node1 = header;
                node2 = node1.forward[i];
                while (!node2.isTail && comparer.Compare(node2.value, retval) <= 0 && node2 != retNode)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                update[i] = node1;
            }


          
            node2 = node1.forward[0];


            //make sure we ahve a pointer to the node with the value
            while (!(node2 = node2.forward[0]).isTail && node2 != retNode)
            {
                node2 = node2.forward[0];
            }

            lock (node2.nodeLock)
            {
                retval = node2.value;
                for (int i = node2.forward.Length - 1; i >= 0; i--)
                {
                    node1 = getLock(update[i], retval, i, ref lockTaken, retNode);
                   
                    lock (node2.levelLock[i])
                    {
                        update[i].forward[i] = node2.forward[i];
                        node2.forward[i] = update[i];
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
            if (header.forward[0].isTail)
                throw new NoSuchItemException();

            T retval;
            int marked = -1;
            Node node1;
            Node node2;
            Node[] update = new Node[maxLevel];
            bool lockTaken = false;
            var time = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;


            node1 = header.forward[0];
            //search until unmarked (ei. 0) node found, and while the node is not currently being deleted
            while (!node1.isTail)
            {
                //if the node was inserted before the search began
                if (node1.timeStamp <= time)
                {
                    marked = Interlocked.Exchange(ref node1.deleted, 1);
                    if (marked == 0)
                    {
                        break;
                    }

                }
                node1 = node1.forward[0];

            }
            Interlocked.Exchange(ref node1.pid, Thread.CurrentThread.ManagedThreadId);
            if (node1 == header)
            {
                throw new Exception("node1 is header, the header should never be marked for deletion");
            }


            //SAVE the value of the node that we just marked for deletion
            retval = node1.value;
            Node retNode = node1;

            node1 = header;
            for (int i = maxLevel - 1; i >= 0; i--)
            {
                node2 = node1;
                while (!node2.forward[i].isTail && comparer.Compare(node2.forward[i].value, retval) < 0)
                {
                    node1 = node2;
                    node2 = node1.forward[i];
                }
                while (node2 != retNode && !node2.forward[i].isTail)
                {
                    node1 = node2;
                    node2 = node1.forward[i];
                    //Array.Clear(update, 0, update.Length);
                }
                update[i] = node1;
            }
            
            node2 = node1.forward[0];
            lock (node2.nodeLock)
            {
                for (int i = node2.forward.Length - 1; i >= 0; i--)
                {
                    node1 = getMinLock(update[i], retval, i, ref lockTaken, node2);
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
            if (header.forward[0].isTail)
                throw new NoSuchItemException();

            Node x = header;
            for (int i = level; i >= 0; i--)
            {
                while (!x.forward[i].isTail && !x.forward[i].forward[i].isTail)
                    x = x.forward[i];
            }
            return x.forward[0].value;
        }

        public T FindMin()
        {
            if (header.forward[0].isTail)
                throw new NoSuchItemException();
            return header.forward[0].value;
        }

        public bool IsEmpty()
        {
            if (header.forward[0].isTail)
                return true;
            return false;
        }
    
        #region helpers

        private Node[] maxSearch(Node head, Node[] update)
        {
            for (int i = head.forward.Length - 1; i >= 0; i--)
            {
                while (head.forward[i].isTail != true && comparer.Compare(head.forward[i].value, head.value) >= 0)
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
                while (head.forward[i].isTail != true && comparer.Compare(head.forward[i].value, head.value) >= 0)
                {
                    if (head.forward[0].isTail == true || head.forward[0].timeStamp > searchStartTime)
                    {
                        return head;
                    }
                    head = head.forward[i];
                }
            }
            return head;
        }
        private Node getInsertionLock(Node node1, T value, int lvl, ref bool firstlocktaken)
        {
            Node node2 = node1.forward[lvl];
            while (node2.isTail != true && comparer.Compare(node2.value, value) <= 0)
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
            while (node2.isTail != true && comparer.Compare(node2.value, value) <= 0)
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

        private Node getLock(Node node1, T value, int lvl, ref bool firstlocktaken, Node retNode)
        {
            Node node2 = node1.forward[lvl];
            while (node2.isTail != true && comparer.Compare(node2.value, value) < 0 && node2 != retNode)
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
            while (node2.isTail != true && comparer.Compare(node2.value, value) < 0)
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
            while (node2.isTail != true && node2 != retNode)
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
            while (node2.isTail != true && node2 != retNode)
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

        private int RandomLevel()
        {
            var rng = new Random();
            var lvl = 1;
            while (rng.Next(2) == 1)
            {
                lvl++;
            }
            if (lvl > maxLevel)
            {
                return maxLevel;
            }
            return lvl;
        }

        //private int RandomLevel()
        //{
        //    var rng = new Random();
        //    var lvl = 1;
        //    while (random.Next(2) == 1 && lvl < maxLevel)
        //    {
        //        lvl++;
        //    }
        //    return lvl;
        //}
    }
    #endregion
}
