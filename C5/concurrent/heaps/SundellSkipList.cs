using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace C5.concurrent.heaps
{
    public class SundellSkipList<T> : IConcurrentPriorityQueue<T>
    {
        public class Node
        {
            private int key, level, validLevel;
            private Node next;
            private Node previous;
            private T value;

            #region getters and setters
            public int Key
            {
                get { return key; }
                set { key = value; }
            }

            public T Value
            {
                get { return value; }
                set { this.value = value; }
            }

            public int Level
            {
                get { return level; }
                set { level = value; }
            }

            public int ValidLevel
            {
                get { return validLevel; }
                set { validLevel = value; }
            }

            public Node[] Next
            {
                get
                {
                    return next;
                }

                set
                {
                    next = value;
                }
            }
            #endregion
        }


        //global varaibles
        private Node head;
        private Node tail;

        //local var
        private Node node2;

        private static int size;
        public Node[] nodes;

        //helpers
        private Node ReadNext(Node node1, int level)
        {

            throw new NotImplementedException();
        }

        private Node ScanKey(Node node1, int level, int key)
        {
            node2 = ReadNext(node1, level);
            while (true)
            {
                
            }
        }

       

        public int Count
        {
            get { return size; }
        }

        public Node[] Nodes
        {
            get
            {
                return nodes;
            }

            set
            {
                nodes = value;
            }
        }

        //Insert
        public bool Add(T item)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<T> All()
        {
            throw new NotImplementedException();
        }

        public bool Check()
        {
            throw new NotImplementedException();
        }

        public T DeleteMax()
        {
            throw new NotImplementedException();
        }

        public T DeleteMin()
        {
            throw new NotImplementedException();
        }

        public T DeleteHelper()
        {
            throw  new NotImplementedException();
        }

        public T FindMax()
        {
           throw new NotImplementedException();
        }

        public T FindMin()
        {
            throw new NotImplementedException();
        }

        public bool IsEmpty()
        {
            throw new NotImplementedException();
        }

        private bool add(T item)
        {
            throw new NotImplementedException();
        }


        private bool check(int i, T min, T max)
        {
            throw new NotImplementedException();
        }

    }
}
