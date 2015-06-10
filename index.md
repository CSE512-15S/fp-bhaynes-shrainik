#HybridPerfopticon
===============

## Team Members

1. Brandon Haynes bhaynes@cs.washington.edu
2. Shrainik Jain shrainik@cs.washington.edu

## Resources

-	We presented a [poster](https://github.com/CSE512-15S/fp-bhaynes-shrainik/blob/master/final/poster.pdf) at CSE-512 poster session. 
-	A paper describing perfopticon is detail can be found [here](https://github.com/CSE512-15S/fp-bhaynes-shrainik/blob/master/final/paper.pdf). 


## Abstract
Hybrid databases have recently gained a lot of attention because of the promise they show in several prominent use cases in scientific data management. A hybrid distributed database system usually consists of multiple underlying databases with differing data models. In order to better understand such systems we present a query profiling and visualization tool for queries authored over these hybrid systems. Our approach is to extend an existing visualization tool for distributed databases  by adding support for multiple underlying database systems.

## In action

![Screenshot of HybridPerfopticon](screenshot.png =350x)

### Demo

A running version of HybridPerfopticon is hosted [here](http://ec2-52-5-229-118.compute-1.amazonaws.com/profile?queryId=21433). 

## Running Instructions

The [Myria](http://myria.cs.washington.edu) database management system leverages Perfopticon as a query visualization tool (via the [Myria webserver](https://github.com/uwescience/myria-web) project).  Since this project is a Perfopticon extension, we therefore elected to extend `Myria-Web` to demonstrate its features.

Accordingly, the installation process requires the following steps:

### Prerequisites

1. Install the [Google App Engine SDK for Python](https://developers.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python)

### Installation

1. Clone this [repository](https://github.com/CSE512-15S/fp-bhaynes-shrainik.git) (`git clone https://github.com/CSE512-15S/fp-bhaynes-shrainik.git`)
2. Update repository submodules (`git submodule init` and `git submodule update`)
3. Then setup the module as described in the Raco [README](https://github.com/uwescience/raco/blob/master/README.md).
4. The PLY library used to parse programs in the Myria language uses a precompiled parsetab.py in the `raco` submodule. This file is not required, but dramatically speeds up the parser load time (which happens for every request to the app). To generate it, run `scripts/myrial examples/reachable.myl`
5. (Optional)  Update `appengine/myria_web_main.py` to point to a specific MyriaX engine (default=`demo.myria.cs.washington.edu`)
6. Launch the webserver (`dev_appserver.py /path/to/myria-web/appengine`)
7. Execute a query (`http://localhost:8080/editor`), making sure to select the profiling option
8. View query profiling results

