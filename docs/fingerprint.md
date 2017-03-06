## Fingerprint Structure

HBase指纹库结构

create 'fingerprint',’chunkInfo’,’fileInfo’

| id  | hashValue | fileNum | chunkNum | chunkSize | file  | offset | content |
| --- | --------- | ------- | -------- | --------- | ----- | ------ | ------- |
| 1   | dasdasda1 | dasdasd | dasdasda |  dasdasda | dasd1 | dasdsa | dsadsad |
| 2   | dasdasda2 | dasdasd | dasdasda |  dasdasda | dasd1 | dasdsa | dasdas  |
| 3   | dasdasda3 | dasdasd | dasdasda |  dasdasda | dasd3 | dasdsa | dsaddsa |
| 4   | dasdasda4 | dasdasd | dasdasda |  dasdasda | dasd4 | dasdsa | dasdasd |
| 5   | dasdasda5 | dasdasd | dasdasda |  dasdasda | dasd5 | dasdsa | dsadas  |
| 6   | dasdasda6 | dasdasd | dasdasda |  dasdasda | dasd6 | dasdsa | dasdas  |

| fileName  | chunkList |
| --------- | --------- |
| dasdasdas | dasdasda  |
