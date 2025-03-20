# dwib-mmi-24 

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents (ToC)</summary>
  <ol>
    <li><a href="#dokumen-kebutuhan-bisnis">Dokumen Kebutuhan Bisnis</a></li>
    <li><a href="#diagram-skema">Diagram Skema Data Warehouse</a></li>
    <li><a href="#script-sql">Script SQL DDL</a></li>
    <li><a href="#deskripsi-dataset">Deskripsi Dataset</a></li>
    <li><a href="#script-etl">Script ETL Python</a></li>
  </ol>
</details>

<!-- Deskripsi Dataset -->
## Deskripsi Dataset
Nama Dataset: ([Brazilian Stock market](https://www.kaggle.com/datasets/leomauro/brazilian-stock-market-data-warehouse))

Dataset ini menggunakan Fact Constellation Schema (Galaxy Schema), dengan detail:

Tabel fakta:
factStocks: Menyimpan transaksi saham harian
factCoins: Menyimpan nilai tukar mata uang harian

Tabel dimensi:
dimCompany: Menyimpan informasi perusahaan terkait
dimCoin: Menyimpan informasi mata uang terkait
dimTime: Menyimpan informasi waktu transaksi

detail file: [Deskripsi Dataset.pdf](deskripsi-dataset.pdf).
