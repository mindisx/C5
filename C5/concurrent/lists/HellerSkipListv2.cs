using System;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace C5.concurrent
{
    public class HellerSkipListv2<T> : IConcurrentPriorityQueue<T>
    {

        internal class Node
        {
            internal object nodeLock = new object();
            internal Node[] forward;
            internal T value;
            internal bool marked;
            internal int level = 0;
            internal bool tail = false;
            public Node(int level, T value)
            {
                this.forward = new Node[level + 1];
                this.value = value;
                marked = false;
                this.level = level;
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

        public HellerSkipListv2() : this(32) { }

        public HellerSkipListv2(int maxlevel)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            header = null;
            int max = 1;
            while (max < maxlevel)
                max <<= 1;
            maxLevel = max;
            tail = new Node(maxLevel - 1, default(T));
            tail.tail = true;
            header = new Node(maxLevel - 1, default(T));
            for (int i = 0; i < maxLevel; i++)
            {
                header.forward[i] = tail;
                tail.forward[i] = tail;
            }
        }


        public int Count
        {
            get
            {
                return size;
            }
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

        public bool Add(T item)
        {
            int lvl = randomLevel();
            bool initialInsert = false;
            Node curr = new Node(-1, item);
            for (int l = 0; l <= lvl; l++)
            {
                bool levelInsert = false;
                while (true)
                {
                    if (curr.marked)
                        return true;
                    Node x = header;
                    Node[] preds = new Node[maxLevel], succs = new Node[maxLevel];
                    if (!initialInsert)
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
                        Node next;
                        bool hittail = false;
                        for (int i = maxLevel - 1; i >= 0; i--)
                        {
                            while (!(next = x.forward[i]).tail && comparer.Compare(next.value, item) < 0)
                                x = next;
                            preds[i] = x;
                            succs[i] = x.forward[i];
                        }

                        x = preds[l - 1];
                        if (x.tail) continue;
                        while (!curr.marked && !(next = x.forward[l - 1]).tail && !curr.Equals(next))
                        {
                            x = next;
                            if (x.tail && x.Equals(curr))
                            {
                                hittail = true;
                                break;
                            }
                            if (x.forward.Length - 1 >= l)
                            {
                                preds[l] = x;
                                succs[l] = x.forward[l];
                            }
                        }
                        if (hittail) continue;
                    }
                    bool breaking = false;
                    for (int ll = l; ll <= lvl; ll++)
                    {
                        lock (preds[ll].nodeLock)
                        {
                            Node pred = preds[ll];
                            if (pred.tail) break;
                            lock (curr.nodeLock)
                            {
                                if (curr.marked) return true;
                                for (int lll = ll; lll < pred.forward.Length; lll++)
                                {
                                    if (lll <= lvl)
                                    {
                                        if (Validate(pred, succs[l], l, item))
                                        {
                                            l = ll = lll;
                                            Node[] newfor = new Node[l + 1];
                                            for (int i = 0; i < curr.forward.Length; i++)
                                                newfor[i] = curr.forward[i];
                                            Interlocked.Exchange(ref newfor[l], succs[l]);
                                            Interlocked.Exchange(ref curr.forward, newfor);
                                            Interlocked.Exchange(ref pred.forward[l], curr);
                                            levelInsert = true;
                                            curr.level = l;
                                            if (l == 0)
                                            {
                                                Interlocked.Increment(ref size);
                                                initialInsert = true;
                                            }
                                        }
                                        else
                                        {
                                            breaking = true;
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        breaking = true;
                                        break;
                                    }
                                }
                                if (breaking)
                                    break;
                            }
                        }
                    }
                    if (levelInsert) break; //continue with next level
                }
            }
            return true;
        }
        public T DeleteMax()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();
            int l = int.MaxValue;
            Node curr = null;
            while (true)
            {
                if (header.forward[0].tail)
                    throw new NoSuchItemException();
                Node x = header;
                Node[] preds = new Node[maxLevel], succs = new Node[maxLevel];
                for (int i = maxLevel - 1; i >= 0; i--)
                {
                    while (x.level >= 0 && !x.forward[i].tail && !x.forward[i].forward[i].tail)
                        x = x.forward[i];
                    preds[i] = x;
                    succs[i] = x.forward[i];
                }


                //lock (preds[0].nodeLock)
                //{
                if (preds[0].marked || preds[0].tail || preds[0].level < 0 || (curr = preds[0].forward[0]).tail)
                    continue;
                //lock (preds[0].forward[0])
                //{
                if (!curr.forward[0].tail || (l = curr.level) < 0)
                {
                    curr = null;
                    continue;
                }

                //    }
                //}
                bool breaking = false;
                for (int ll = l; ll >= 0; ll--)
                {
                    lock (preds[l].nodeLock)
                    {
                        if (preds[l].tail || preds[l].level < 0)
                        {
                            curr = null;
                            break;
                        }
                        lock (curr.nodeLock)
                        {

                            if (curr.tail || !curr.forward[0].tail || curr.level < 0)
                            {
                                curr = null;
                                break;
                            }
                            if (curr.level != l) //new stuff
                            {
                                ll = l = curr.level;
                                continue;
                            }
                            while (true)
                            {
                                if (ValidatePred(preds[l], curr, l) && curr.Equals(succs[l]))
                                {
                                    curr.marked = true;
                                    Interlocked.Exchange(ref preds[l].forward[l], curr.forward[l]);
                                    curr.level = ll = --l;
                                    if (l == -1)
                                    {
                                        Interlocked.Decrement(ref size);
                                        return curr.value;
                                    }
                                    if (!preds[l + 1].Equals(preds[l]))
                                    {
                                        breaking = true;
                                        break;
                                    }
                                }
                                else
                                {
                                    breaking = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (breaking)
                        break;
                }
            }
        }

        public T DeleteMin()
        {
            T retavl = default(T);
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
                        if (ValidateUnmarked(pred, curr, 0))
                        {
                            curr.marked = true;
                            retavl = curr.value;
                            for (int i = curr.forward.Length - 1; i >= 0; i--)
                            {
                                Interlocked.Exchange(ref pred.forward[i], curr.forward[i]);
                                currlvl = i;
                                curr.level = i;
                                if (i == 0)
                                    curr.level = -1;
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
            while (true)
            {
                if (header.forward[0].tail)
                    throw new NotImplementedException();
                Node x = header;
                for (int i = maxLevel - 1; i >= 0; i--)
                {
                    while (!x.forward[i].tail && !x.forward[i].forward[i].tail)
                        x = x.forward[i];
                }
                if (!x.tail || !x.forward[0].tail)
                    return x.forward[0].value;
            }
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

        private int randomLevel()
        {
            Random rng = new Random();
            int level = 0;
            int p = 1;
            while (rng.Next(2) < p && level < maxLevel - 1)
                level = level + 1;
            return level;
        }

        private bool Validate(Node pred, Node succ, int level, T item)
        {
            if (succ.tail && !pred.marked && pred.forward[level].Equals(succ) && comparer.Compare(pred.value, item) <= 0)
                return true;
            if (!succ.tail && !pred.marked && !succ.marked && pred.forward[level].Equals(succ) &&
                comparer.Compare(pred.value, item) <= 0 && comparer.Compare(succ.value, item) >= 0)
                return true;
            return false;
        }

        private bool ValidateUnmarked(Node pred, Node succ, int level)
        {
            if (!succ.tail && !pred.marked && !succ.marked && pred.forward[level].Equals(succ))
                return true;
            return false;
        }
        private bool ValidateMarked(Node pred, Node succ, int level)
        {
            if (!succ.tail && !pred.marked && succ.marked && /*succ.pid == Thread.CurrentThread.ManagedThreadId &&*/ pred.forward[level].Equals(succ))
                return true;
            return false;
        }

        private bool ValidatePred(Node pred, Node succ, int level)
        {
            if (!succ.tail && !pred.marked && pred.forward[level].Equals(succ))
                return true;
            return false;
        }
    }
}
