# Project Overview
This project serves as both a real-time arbitrage detection system, and a historical analysis of the crypto market.

# Project Layout

### Interactive Dashboards
The interactive dashboards contain both a real-time and historical view of crypto arbitrage opportunities. More info can
be found in the `interactive_dashboards` directory.

### Notebooks
The notebooks contain the code for the historical analysis of the crypto market.

### Python Scripts
The python scripts contain the code for aggregating our historical data.

# Setup

To run the project, you will need three prerequisites:
1. python version 3.14+
2. node version 18+
3. pnpm version 9+

You can verify these by running the following commands in your terminal:
> $ python --version

> $ node --version

> $ pnpm --version

Once you have these installed in your machine, you can begin setting up the project.

## Notebooks
To run the notebooks, install all packages listed in the `requirements.txt` root file, either in a virtual environment or
directly in your machine.

That is all the setup required, allowing you to successfully run all notebooks.

## Interactive Dashboards
To run the interactive dashboards, there is a bit more setup required. 

### 1.
Set `interactive_dashboards` as your working directory by running in your terminal
> $ cd interactive_dashboards


### 2.
Install the root dependencies by running
> $ pnpm install

### 3. 
To access the Live Dashboard, run
> $ cd apps/frontend

Ensure you have all live dashboard dependencies installed by running
> $ pnpm install

Once all dependencies are installed, you can run the Live Dashboard with
> $ pnpm dev

This will initialize the Live Dashboard in localhost port 5173. Accessing the link will take you to the main dashboard view.
You may see an empty dashboard indicating no data available. In the top right corner is the tab to switch between Live and 
Historical mode. 

Click on `Live` mode. This will populate the page with various nodes, filters, and configurations. You can read more about
these in the various README files in the `interactive_dashboards/` directory.

### 4.
To access the Historical Dashboard, open a new terminal window and set `/backend` as your working directory. You'll
likely need to run:
> $ cd interactive_dashboards/apps/backend

Once again, ensure all dependencies are installed by running
> $ pnpm install

And then initialize the Historical Dashboard by running
> $ pnpm dev

You'll see in the terminal a message indicating the server is running on port 3001. However, if you open the link, you'll
find nothing there. This is because port 3001 is simply an API.

What you instead want to do is go back to the Live Dashboard page in port 5173, and click on the `Historical` tab on the
top right. This should populate the once empty page with the historical data. Again, you can read more about the different
options in the README files in the `interactive_dashboards/` directory.

## Python Scripts
The `python_scripts` directory includes our one-time collection and aggregation of historical data. We advise against trying
to run these scripts, as it will require setting up your own DB. We've left the code here so that you may read it over and
analyze our approach to data collection. 
