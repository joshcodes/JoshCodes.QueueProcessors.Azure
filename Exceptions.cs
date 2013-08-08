using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JoshCodes.QueueProcessors
{
    public class MessageException : Exception
    {
        public MessageException(string message)
            : base(message)
        {
        }
    }

    public class ReprocessMessageException : MessageException
    {
        public ReprocessMessageException(string message)
            : base(message)
        {
        }
    }

    public class BrokenMessageException : MessageException
    {
        private Exception exception;

        public BrokenMessageException(string message, Exception exception)
            : base(message)
        {
            this.exception = exception;
        }

        public BrokenMessageException(string message)
            : base(message)
        {
        }
    }
}
