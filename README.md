CSE-512 Final Project: HybridPerfopticon
===============

## Team Members

1. Brandon Haynes bhaynes@cs.washington.edu
2. Shrainik Jain shrainik@cs.washington.edu

## Summary
HybridPerfopticon extends Perfopticon to support hybrid databases. This required significant system building effort and understanding the exisiting perfopticon architecture. We needed a mechanism to find equivalence betweent the SciDB world and Myria world. Since the project had to be scoped down to 3 weeks, we made certain assumptions and tried to use the existing architectures as much as possible. We still needed an interface between Myria and SciDB, a rest Server to serve the hybrid plans and other profiling requests from perfopticon. Also, since Brandon had already [extended Perfopticon]() to show dynamic visualizations (tuple flow info. etc.) we built on top of that rather than the vannila perfopticon. 

## Design Process and Work Breakdown

| Task | Task Breakup | Notes |
|---|---| ---|
|Literature Review| Hybrid Databases: Brandon, Visualization: Both| Prior to the project, Brandon was already working on a middleware for hybrid databases, literature review for that was already finished and used in this project. Since the work is an extension to PerfOpticon, the literature review from a visualization point of view was also built on literature review during the PerfOpticon project. |
|Identify target databases| Brandon| We used SciDB and Myria systems to present a proof of concept for visualization. 
|Whiteboarding and Design| Both| |
|Building the infrastructure hybrid database|Mostly Brandon, Design by Both|The proof of concept requires a bare bones working hybrid database query platform, we used and built upon the Myria Middleware as necessary.|
|Extend SciDB to write perf logs|Brandon, design: Both| Perfopticon ingests logs in a specific format, SciDB needs to be extended to support this. We later realized that we could just use SciDB's existing logs as it was better suited for the scope of 3 weeks and didn't involve potentially dealing with SciDB debugging issues. We also needed a mechanism to ask for query logs from the Myria webserver, this was done via a simple rest server interfacing with SciDB|
|Visualization: Distinctly point out SciDB parts of the query plan| Shrainik| Ingest the overall hybrid plan and aid the user by showing the two component databases in the plan separately via a color encoding|
|Visualization: Dynamic visualization for SciDB parts of the query plan|Both| Extended the dynamic visualization (tuple flow etc.) to support SciDB fragments as well.|
|Visualization: Bug Fixes, Incorporating feedback|  Both| There are some minor bugs which make the visualization difficult to use sometimes.|


## Running Instructions

The [Myria](http://myria.cs.washington.edu) database management system leverages Perfopticon as a query visualization tool (via the [Myria webserver](https://github.com/uwescience/myria-web) project).  Since this project is a Perfopticon extension, we therefore elected to extend `Myria-Web` to demonstrate its features.

Accordingly, the installation process requires the following steps:

### Prerequisites

1. Install the [Google App Engine SDK for Python](https://developers.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python)

### Installation

1. Clone this [repository](https://github.com/CSE512-15S/a3-haynesb) (`git clone https://github.com/CSE512-15S/a3-haynesb.git`)
2. Update repository submodules (`git submodule init` and `git submodule update`)
3. Then setup the module as described in the Raco [README](https://github.com/uwescience/raco/blob/master/README.md).
4. The PLY library used to parse programs in the Myria language uses a precompiled parsetab.py in the `raco` submodule. This file is not required, but dramatically speeds up the parser load time (which happens for every request to the app). To generate it, run `scripts/myrial examples/reachable.myl`
5. (Optional)  Update `appengine/myria_web_main.py` to point to a specific MyriaX engine (default=`demo.myria.cs.washington.edu`)
6. Launch the webserver (`dev_appserver.py /path/to/myria-web/appengine`)
7. Execute a query (`http://localhost:8080/editor`), making sure to select the profiling option
8. View query profiling results

