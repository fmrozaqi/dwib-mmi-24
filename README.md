# dwib-mmi-24

- Alfy Nur Fauzia 24/546719/PPA/06850
- Faiz Miftakhur Rozaqi 24/548072/PPA/06915
- Firda Ayu Safitri 24/548483/PPA/06929
- Silmi Utami Putri 24/549092/PPA/06935

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

- factStocks: Menyimpan transaksi saham harian
- factCoins: Menyimpan nilai tukar mata uang harian

Tabel dimensi:

- dimCompany: Menyimpan informasi perusahaan terkait
- dimCoin: Menyimpan informasi mata uang terkait
- dimTime: Menyimpan informasi waktu transaksi

detail file: [Deskripsi Dataset.pdf](Deskripsi%20Dataset.pdf).
