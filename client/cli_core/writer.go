package cli_core

import (
	"bufio"
	"net"
	"os"
	"strconv"
	"strings"
)

// test for bulk request
// const s string = "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"

func StartWriter(conn net.Conn, stop chan error) {
	reader := bufio.NewReader(os.Stdin)
	for {
		bytes, _, err := reader.ReadLine()
		req := strings.Split(string(bytes), " ")
		send := []byte(packBulkArray(req))
		// bulk
		if err != nil {
			stop <- err
			break
		}
		// INLINE
		// BULK Request is also supported
		length := len(send)
		index := 0
		for index < length {
			n, err := conn.Write(send[index:])
			if err != nil {
				stop <- err
				break
			}
			index += n
		}
		// log.Printf("[CLIENT WRITER] Send message %s success\n", s)
	}
}

// const s string = "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"

func packBulkArray(req []string) string {
	sCnt := strconv.FormatInt(int64(len(req)), 10)
	s := strings.Builder{}
	s.WriteRune('*')
	s.WriteString(sCnt)
	s.WriteString(CRLF)
	for _, r := range req {
		s.WriteRune('$')
		s.WriteString(strconv.FormatInt(int64(len(r)), 10))
		s.WriteString(CRLF)
		s.WriteString(r)
		s.WriteString(CRLF)
	}
	return s.String()
}
