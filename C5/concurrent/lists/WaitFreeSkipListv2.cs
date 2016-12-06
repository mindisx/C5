using System;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace C5.concurrent
{
    public class WaitFreeSkipListv2<T> : IConcurrentPriorityQueue<T>
    {

        internal class Node
        {
            internal Node[] forward;
            internal T value;
            internal bool marked;
            internal object nodeLock = new object();
            internal int pid = 0;
            internal bool tail = false;
            public Node(int level, T value)
            {
                this.forward = new Node[level + 1];
                this.value = value;
                marked = false;
            }
            public override string ToString()
            {
                return string.Format("[{0} - {1}]", value, marked);
            }
        }

        object levelLock = new object();
        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        int size, maxLevel;
        Node header, tail;
        Random rng;

        public WaitFreeSkipListv2() : this(32) { }

        public WaitFreeSkipListv2(int maxlevel)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            header = null;
            int max = 1;
            while (max < maxlevel)
                max <<= 1;
            maxLevel = max;
            tail = new Node(-1, default(T));
            tail.tail = true;
            header = new Node(maxlevel - 1, default(T));
            for (int i = 0; i < header.forward.Length; i++)
                header.forward[i] = tail;
            rng = new Random();
        }


        public int Count
        {
            get
            {
                return size;
            }
        }

        public bool Add(T item)
        {
            int lvl = randomLevel();
            bool initialinsert = false;
            Node n = new Node(0, item);
            for (int l = 0; l <= lvl; l++)
            {
                bool levelinsert = false;
                while (true)
                {
                    Node x = header;
                    Node[] preds = new Node[maxLevel], succs = new Node[maxLevel];
                    try
                    {
                        if (!initialinsert)
                        {
                            for (int i = maxLevel - 1; i >= 0; i--)
                            {
                                while (!x.forward[i].tail && comparer.Compare(x.forward[i].value, item) <= 0)
                                    x = x.forward[i];
                                preds[i] = x;
                                succs[i] = x.forward[i];
                            }
                        }
                        else
                        {
                            for (int i = maxLevel - 1; i >= 0; i--)
                            {
                                while (!x.forward[i].tail && comparer.Compare(x.forward[i].value, item) < 0)
                                    x = x.forward[i];
                                preds[i] = x;
                                succs[i] = x.forward[i];
                            }

                            Node npred = preds[l];
                            x = preds[l - 1];
                            while (!n.marked && !n.Equals(x.forward[l - 1]))
                            {
                                x = x.forward[l - 1];
                                if (x.forward.Length - 1 >= l)
                                    npred = x;
                            }
                            preds[l] = npred;
                            succs[l] = npred.forward[l];
                        }
                    }
                    catch (IndexOutOfRangeException e)
                    {
                        continue;
                    }
                    lock (preds[l].nodeLock)
                    {
                        lock (n.nodeLock)
                        {
                            if (n.tail)
                                continue;
                            if (n.marked)
                                return true;
                            if (Validate(preds[l], succs[l], l, item))
                            {
                                if (n.forward.Length - 1 < l)
                                {
                                    Node[] newfor = new Node[l + 1];
                                    for (int i = 0; i < n.forward.Length; i++)
                                    {
                                        newfor[i] = n.forward[i];
                                    }
                                    Interlocked.Exchange(ref newfor[l], preds[l].forward[l]);
                                    Interlocked.Exchange(ref n.forward, newfor);
                                }
                                Interlocked.Exchange(ref n.forward[l], preds[l].forward[l]);
                                Interlocked.Exchange(ref preds[l].forward[l], n);
                                levelinsert = true;
                                if (l == 0)
                                {
                                    Interlocked.Increment(ref size);
                                    initialinsert = true;
                                }
                            }
                        }
                    }
                    if (levelinsert)
                        break; //continue with next level
                }
            }
            return true;
        }

        private bool Validate(Node pred, Node curr, int level, T item)
        {
            if (!curr.tail && comparer.Compare(curr.value, item) < 0)
                return false;
            if (!pred.marked && comparer.Compare(pred.value, item) <= 0 && !curr.marked && pred.forward[level].Equals(curr))
                return true;
            if (!pred.marked && comparer.Compare(pred.value, item) <= 0 && pred.forward[level].Equals(curr))
                return true;
            return false;
        }

        private bool ValidateUnmarked(Node pred, Node curr, int level)
        {
            if (!pred.marked && !curr.tail && curr.pid == 0 && !curr.marked && curr.Equals(pred.forward[level]))
                return true;
            return false;
        }
        private bool ValidateMarked(Node pred, Node curr, int level)
        {
            if (!pred.marked && !curr.tail && !pred.forward[level].tail && pred.forward[level].Equals(curr))
                return true;
            return false;
        }


        public SCG.IEnumerable<T> All()
        {
            throw new NotImplementedException();
        }
        public bool Check()
        {
            if (size == 0)
                return true;

            for (int i = 0; i <= maxLevel - 1; i++)
            {
                Node x = header;
                //header is 0 (and we allow for dupliates as of 21/11/2016), so we have to check if the previous node is greater then or equel to the current one
                while (!x.forward[i].tail && comparer.Compare(x.value, x.forward[i].value) <= 0)
                    x = x.forward[i];

                //If the the next element isen't null, there's an element next x (the supposed last and least element), and the list is out of order
                if (!x.forward[i].tail)
                    return false;

            }
            return true;
        }

        public T DeleteMax()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();
            T retval = default(T);
            bool initialdelete = false;
            int lvl = int.MaxValue, currlvl = int.MaxValue;
            Node n = null;
            while (true)
            {

                Node x = header;
                Node[] preds = new Node[maxLevel];
                try
                {
                    if (!initialdelete)
                    {
                        for (int i = maxLevel - 1; i >= 0; i--)
                        {
                            while (!x.forward[i].tail && !x.forward[i].forward[i].tail)
                                x = x.forward[i];
                            preds[i] = x;
                        }
                    }
                    else
                    {
                        for (int i = maxLevel - 1; i >= 0; i--)
                        {
                            while (!x.forward[i].tail && !x.forward[i].forward[i].tail && comparer.Compare(x.forward[i].value, retval) < 0)
                                x = x.forward[i];
                            preds[i] = x;
                        }
                        Node npred = preds[lvl];
                        x = preds[lvl];

                        while (!x.forward[lvl].tail && !n.Equals(x.forward[lvl]))
                        {
                            x = x.forward[lvl];
                            if (x.forward.Length - 1 >= lvl)
                                npred = x;
                        }
                        preds[lvl] = npred;
                    }

                    if (!initialdelete)
                    {
                        lock (preds[0].nodeLock)
                        {
                            n = preds[0].forward[0];
                            lock (n.nodeLock)
                            {
                                if (n.tail)
                                    continue;

                                if (!n.tail && !n.marked && n.pid == 0 && ValidateUnmarked(preds[0], n, 0))
                                {
                                    retval = n.value;
                                    n.pid = Thread.CurrentThread.ManagedThreadId;
                                    n.marked = true;
                                    lvl = n.forward.Length - 1;
                                    initialdelete = true;
                                }
                                else
                                    continue;
                            }
                        }
                    }
                }
                catch (IndexOutOfRangeException e)
                {
                    continue;
                }

                lock (preds[lvl].nodeLock)
                {
                    if (preds[lvl].tail)
                        continue;
                    lock (n.nodeLock)
                    {
                        if (n.tail)
                            continue;
                        if (!initialdelete)
                        {
                            if (!n.tail && n.marked && n.pid == Thread.CurrentThread.ManagedThreadId && ValidateMarked(preds[lvl], n, lvl))
                            {
                                Interlocked.Exchange(ref preds[lvl].forward[lvl], n.forward[lvl]);
                                lvl--;
                                currlvl = lvl;
                            }
                            else
                                continue;
                        }
                        else
                        {
                            if (!n.tail && n.marked && n.pid == Thread.CurrentThread.ManagedThreadId && ValidateMarked(preds[lvl], n, lvl))
                            {
                                Interlocked.Exchange(ref preds[lvl].forward[lvl], n.forward[lvl]);
                                lvl--;
                                currlvl = lvl;
                            }
                            else
                                continue;
                        }
                    }
                }
                if (currlvl == -1)
                    break;
            }

            Interlocked.Decrement(ref size);
            return retval;
        }

        public T DeleteMin()
        {

            if (header.forward[0].tail)
                throw new NoSuchItemException();
            T retavl;
            int currlvl = int.MaxValue;
            while (true)
            {
                Node pred = header;
                lock (pred.nodeLock)
                {
                    Node curr = header.forward[0];
                    lock (curr.nodeLock)
                    {
                        if (curr.tail)
                            throw new NoSuchItemException();
                        retavl = curr.value;
                        if (ValidateUnmarked(header, curr, 0))
                        {
                            curr.marked = true;
                            curr.pid = Thread.CurrentThread.ManagedThreadId;
                            for (int i = curr.forward.Length - 1; i >= 0; i--)
                            {
                                Interlocked.Exchange(ref pred.forward[i], curr.forward[i]);
                                currlvl = i;
                            }
                        }
                    }
                }
                if (currlvl == 0)
                    break;
            }
            Interlocked.Decrement(ref size);
            return retavl;
        }

        public T FindMax()
        {
            if (header.forward[0] == null)
                throw new NotImplementedException();

            Node x = header;
            while (x.forward[0] != null && x.forward[0].forward[0] != null)
                x = x.forward[0];
            return x.forward[0].value;
        }

        public T FindMin()
        {
            if (header.forward[0] == null)
                throw new NoSuchItemException();

            return header.forward[0].value;
        }

        public bool IsEmpty()
        {
            if (header.forward[0] == null)
                return true;
            return false;
        }

        private int randomLevel()
        {
            int level = 0;
            int p = 1;
            while (rng.Next(2) < p && level < maxLevel - 1)
                level = level + 1;
            return level;
        }
    }
}
