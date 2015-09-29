<?php
/*
 * @package thrift.transport
 */


/**
 * @package thrift.transport
 */
class TSaslClientTransport extends TTransport {

    const START = 1;
    const COMPLETE = 5;

    /**
     * @var TTransport
     */
    protected $transport_;

    protected $wbuffer_, $rbuffer_;

    public function __construct(TTransport $transport) {
        $this->transport_ = $transport;
        $this->wbuffer_ = '';
        $this->rbuffer_ = '';
    }

    /**
     * Whether this transport is open.
     *
     * @return boolean true if open
     */
    public function isOpen() {
        return $this->transport_->isOpen();
    }

    /**
     * Open the transport for reading/writing
     *
     * @throws TTransportException if cannot open
     */
    public function open() {
        if (!$this->isOpen()) {
            $this->transport_->open();
        }

        $mechanism = 'PLAIN';
        $header = pack('CN', self::START, strlen($mechanism));
        $this->transport_->write($header . $mechanism);

        $username = 'sanshengshi';
        $password = '3s0978'; //随意
        $body = chr(0) . $username . chr(0) . $password;

        $header = pack('CN', self::COMPLETE, strlen($body));
        $this->transport_->write($header . $body);

        $data = $this->transport_->readAll(5);
        $array = unpack('Cstatus/Ilength', $data);

        if ($array['status'] != self::COMPLETE) {
            throw new TTransportException();
        }
        return true;
    }

    /**
     * Close the transport.
     */
    public function close() {
        $this->transport_->close();
    }

    /**
     * Read some data into the array.
     *
     * @param int $len How much to read
     * @return string The data that has been read
     * @throws TTransportException if cannot read any more data
     */
    public function read($len) {
        if (strlen($this->rbuffer_) > 0) {
            $ret = substr($this->rbuffer_, 0, $len);
            $this->rbuffer_ = substr($this->rbuffer_, $len);
            return $ret;
        }

        $data = $this->transport_->readAll(4);
        $array = unpack('Nlength', $data);
        $length = $array['length'];

        $this->rbuffer_ = $this->transport_->readAll($length);
        $ret = substr($this->rbuffer_, 0, $len);
        $this->rbuffer_ = substr($this->rbuffer_, $len);
        return $ret;
    }

    /**
     * Writes the given data out.
     *
     * @param string $buf  The data to write
     * @throws TTransportException if writing fails
     */
    public function write($buf) {
        $this->wbuffer_ .= $buf;
    }

    public function flush() {
        $buffer = pack('N', strlen($this->wbuffer_)) . $this->wbuffer_;
        $this->send($buffer);
        $this->wbuffer_ = '';
    }

    public function send($buf) {
        $this->transport_->write($buf);
        $this->transport_->flush();
    }

    public function pack($str) {
        $data = explode(' ', $str);
        $args = array(null);
        $cnt = 0;

        foreach ($data as $v) {
            $v1 = str_split($v, 2);
            foreach ($v1 as $v2) {
                $args[] = hexdec($v2);
                $cnt++;
            }
        }
        $args[0] = str_repeat('C', $cnt);
        $ret = call_user_func_array('pack', $args);
        return $ret;
    }
}
