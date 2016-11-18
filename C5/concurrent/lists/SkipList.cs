using System;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;

namespace C5.concurrent
{
    public class SkipList<T> : IConcurrentPriorityQueue<T>
    {
        public class Node
        {
            //An array the size of the "height" of the node, holds pointers to nodes on the seperate levels. 
            public Node[] forward;
            public T value;
            public Node(int level, T newValue)
            {
                forward = new Node[level];
                value = newValue;
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
        Random random;

        public SkipList() : this(32) { }

        public SkipList(int max)
        {
            comparer = SCG.Comparer<T>.Default;
            itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            maxLevel = max;
            size = 0;
            level = 1;
            header = new Node(maxLevel, default(T));
            random = new Random();

        }
    


        public int Count { get { return size; } }

        public bool Add(T item)
        {
            Node[] update = new Node[maxLevel];
            Node x = header;
            for (int i = level; i >= 0; i--)
            {
                while (x.forward[i] != null && comparer.Compare(x.forward[i].value, item) < 0)
                {
                    x = x.forward[i];
                }
                update[i] = x;
            }
            int newLevel = RandomLevel();
            if (newLevel > level)
            {
                for (int i = level; i < newLevel; i++)
                {
                    update[i] = header;
                }
                level = newLevel;
            }
            x = new Node(newLevel, item);
            for (int i = 0; i < newLevel; i++)
            {
                x.forward[i] = update[i].forward[i];
                update[i].forward[i] = x;
            }
            size++;
            return true;

        }

        public SCG.IEnumerable<T> All()
        {
            if (size == 0)
                throw new NoSuchItemException();

            Node x = header;
            T[] elements = new T[size];
            int i = 0;
            while (x.forward[0] != null)
            {
                elements[i] = x.forward[0].value;
                x = x.forward[0];
                i++;
            }
            return elements;
        }

        public bool Check()
        {
            throw new NotImplementedException();
        }

        public T DeleteMax()
        {
            if (size == 0)
                throw new NoSuchItemException();

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

        public T DeleteMin()
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

        public T FindMax()
        {
            if (size == 0)
                throw new NoSuchItemException();

            Node x = header;
            for (int i = level; i >= 0; i--)
            {
                while (x.forward[i] != null && x.forward[i].forward[i] != null)
                    x = x.forward[i];
            }
            return x.forward[0].value;
        }

        public T FindMin()
        {
            if (size == 0)
                throw new NoSuchItemException();
            return header.forward[0].value;
        }

        public bool IsEmpty()
        {
            if (size == 0)
                return true;
            return false;
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
}
