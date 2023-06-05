# Welcome to companies-ingestion 👋

[![Docker](https://img.shields.io/badge/Docker-20.10.6-blue.svg?cacheSeconds=2592000)](/uri)
[![Pyspark](https://img.shields.io/badge/Pyspark-3.4.0-red.svg?cacheSeconds=2592000)](/uri)
[![Python](https://img.shields.io/badge/Python-3.10-yellow.svg?cacheSeconds=2592000)](/uri)

> Projeto de prospect ingestion - Aqui ficam os jobs pex utilizados para o pipe de ingestao B2B
### 🏠 [Homepage](https://github.com/GeckoVision/companies-ingestion)

Prospect Ingestion
===================================

O objetivo desse projeto, e realizar a ingestao de dados de prospeccao,
permitindo que o fluxo de entrada ocorra de uma forma padronizada, gerando listas
que possibilitem um futuro enriquecimento.


Installation
------------

PyPi
----
Adicione o conteudo do requirements.txt ao seu atual

Em seguida, instale as libs em um novo environment (e.g.):

```bash
    $source ~/venvp3_8/bin/activate
    (venvp3_8) $pip install -r requirements.txt
```


Setup Docker
----
linux ubuntu distros (linux mint in this case)

step 1
```bash
    $sudo apt-get update
    $sudo apt-get install \
          apt-transport-https \
          ca-certificates \
          curl \
          gnupg \
          lsb-release
```

step 2

```bash
    $curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    $echo   "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu focal stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    $sudo apt-get update
```

step 3

```bash
    $sudo apt-get update
    $sudo apt-get install docker-ce docker-ce-cli containerd.io
```

Test install
--------------

```bash
    $sudo docker run hello-world
```

> ref: link - https://docs.docker.com/engine/install/

Run Pex
========

Pipeline prospect ingestion

This ingestion code, is responsible to build the pipeline B2B, and loads the data.

* Primeiro, voce deve ter o docker instalado em sua maquina, favor seguir os passos acima.

Run instructions
-----------------

Para fazer o build da imagem docker em dev, siga o passo abaixo:

```bash
  $ make docker dev
```

Para fazer o build da imagem docker em prod, siga o passo abaixo:
```bash
  $ make docker prod
```

Gerando o build local:

```bash
  $ make clean
```


## Author

👤 **Ernani Britto**