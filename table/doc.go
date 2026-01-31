/*
Package table はシンプルなテーブル機能を提供する。

# 概要

SimpleTableはB-treeをベースにしたテーブル実装。
Tuple（行）を格納し、キーによる検索と範囲スキャンをサポートする。

# Tuple（タプル）

Tupleは複数のバイト列要素からなる行データ：

	tuple := table.Tuple{
	    []byte("1"),      // ID
	    []byte("Alice"),  // Name
	    []byte("25"),     // Age
	}

# キーと値の分離

テーブル作成時に numKeyElems を指定すると、
Tupleの最初のn個の要素がキー、残りが値として扱われる：

	// numKeyElems = 1 の場合
	Tuple: [ID, Name, Age]
	       └──┘ └───────┘
	       Key    Value

	// numKeyElems = 2 の場合
	Tuple: [ID, Name, Age]
	       └───────┘ └──┘
	          Key    Value

# 使用例

	// テーブル作成（最初の1要素がキー）
	tbl, _ := table.Create(bufmgr, 1)

	// 行の挿入
	tbl.Insert(bufmgr, table.Tuple{
	    []byte("1"),
	    []byte("Alice"),
	    []byte("25"),
	})

	// 全件スキャン
	iter, _ := tbl.Scan(bufmgr)
	for {
	    tuple, _ := iter.Next(bufmgr)
	    if tuple == nil {
	        break
	    }
	    fmt.Println(tuple)
	}

	// キーを指定してスキャン
	iter, _ = tbl.ScanFrom(bufmgr, table.Tuple{[]byte("1")})

# データの永続化

SimpleTableはB-treeを使用するため、データは自動的にページに格納される。
bufmgr.Flush()を呼び出すことでディスクに永続化できる。
*/
package table
