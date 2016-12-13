using System;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;

namespace C5.concurrent
{
    public class GlobalLockSkipList<T> : IConcurrentPriorityQueue<T>
    {
        private static readonly object globalLock = new object();

        internal class Node
        {
            internal Node[] forward;
            internal T value;
            public Node(int level, T value)
            {
                this.forward = new Node[level + 1];
                this.value = value;
            }
            public override string ToString()
            {
                return string.Format("[{0}]", value);
            }
        }

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        int size, maxLevel, level;
        Node header;
        Random rng;

        public GlobalLockSkipList() : this(32) { }

        public GlobalLockSkipList(int maxlevel)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            header = null;
            int max = 1;
            while (max < maxlevel)
                max <<= 1;
            maxLevel = max;
            level = 0;
            header = new Node(maxLevel, default(T));
            rng = new Random();
        }


        public int Count
        {
            get
            {
                lock (globalLock)
                {
                    return size;
                }
            }
        }

        public bool Add(T item)
        {
            lock (globalLock)
            {
                try
                {
                    Node[] update = new Node[maxLevel];
                    Node x = header;

                    for (int i = level; i >= 0; i--)
                    {
                        while (x.forward[i] != null && comparer.Compare(x.forward[i].value, item) < 0)
                            x = x.forward[i];
                        update[i] = x;
                    }

                    int lvl = randomLevel();
                    if (lvl > level)
                    {
                        for (int i = level + 1; i <= lvl; i++)
                        {
                            update[i] = header;
                        }
                        level = lvl;
                    }
                    x = new Node(lvl, item);
                    for (int i = 0; i <= lvl; i++)
                    {
                        x.forward[i] = update[i].forward[i];
                        update[i].forward[i] = x;
                    }
                    size++;
                    return true;
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        public SCG.IEnumerable<T> All()
        {
            lock (globalLock)
            {
                if (size == 0)
                    throw new NoSuchItemException();
                T[] items = new T[size];
                Node x = header;
                for (int i = 0; i < size; i++)
                {
                    x = x.forward[0];
                    items[i] = x.value;
                }
                return items;
            }
        }

        public bool Check()
        {
            throw new NotImplementedException();
        }

        public T DeleteMax()
        {
            lock (globalLock)
            {
                try
                {
                    Node[] update = new Node[maxLevel];
                    Node x = header;
                    T retval;
                    for (int i = level; i >= 0; i--)
                    {
                        while (x.forward[i] != null && x.forward[i].forward[i] != null)
                            x = x.forward[i];
                        update[i] = x;
                    }

                    x = x.forward[0];
                    retval = x.value;
                    for (int i = 0; i < x.forward.Length; i++)
                    {
                        update[i].forward[i] = null;
                    }

                    size--;
                    return retval;
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        public T DeleteMin()
        {
            lock (globalLock)
            {
                try
                {
                    if (size == 0)
                        throw new NoSuchItemException();

                    Node x = header.forward[0];
                    T retval = x.value;

                    for (int i = 0; i < x.forward.Length; i++)
                    {
                        header.forward[i] = x.forward[i];
                    }
                    size--;
                    return retval;
                }
                catch (Exception e)
                {
                    throw e;
                }
            }
        }

        public T FindMax()
        {
            lock (globalLock)
            {
                Node x = header;
                for (int i = level; i >= 0; i--)
                {
                    while (x.forward[i] != null && x.forward[i].forward[i] != null)
                        x = x.forward[i];
                }
                return x.forward[0].value;
            }
        }

        public T FindMin()
        {
            lock (globalLock)
            {
                if (size == 0)
                    throw new NoSuchItemException();
                return header.forward[0].value;
            }
        }

        public bool IsEmpty()
        {
            lock (globalLock)
            {
                if (size == 0)
                    return true;
                return false;
            }
        }

        private int randomLevel()
        {
            int level = 0;
            int p = 1;
            while (rng.Next(2) < p && level < maxLevel)
                level = level + 1;
            return level;
        }
    }
}
