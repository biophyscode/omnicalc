
.. _sec-runtime:

Run time
========

In the first two chapters of the documentation, we have described the formulation of an incoming dataset (:ref:`raw data <sec-paths>`) and how to write variables (:ref:`metadata <sec-metadata-basic>`). Understanding how to prepare the data and construct the metadata are necessary to use omnicalc, particularly since its execution is exceedingly simple. Executing omnicalc only requires one command to perform post-processing (there are other commands for plotting and debugging described at the end of this section).

.. code-block :: bash
	
	make compute

The main loop
-------------

The ``make compute`` command triggers omnicalc's main loop, found in the :class:`workspace.py <omni.base.workspace.Workspace>` module, which performs the following functions in order. Note that each of these actions takes its marching orders from the specifications files described in the :ref:`metadata <metadata>` section.

1. Read and merge all of the specifications files found in ``calcs/meta/*.yaml``. Some users may prefer to put the protected top-level dictionaries described in the :ref:`metadata <metadata>` section in separate ``yaml`` files. These files are merged and loaded into the workspace. Internal :ref:`variable <sec-variables>` substitutions are performed at this step.
2. Create slices specified by the top-level ``slices`` dictionary compiled from the metadata. Recall that the creation of :ref:`slices <slices>` will generate both groups (corresponding to GROMACS-style index files) and trajectory files.
3. Run the calculations set in the ``calculations`` dictionary in an order which is inferred by their internal dependencies. This means that a calculation which depends on another will occur later in the loop. Calculation details are interpreted by the wordspace to identify any special ``loop`` settings, which will cause the calculation to be executed many times, across an arbitrary number of parameter sweeps. Each distinct calculation is sent to the :meth:`computer <omni.base.computer.computer>` function, which runs the calculation over all simulations in the collections list. 

The main loop is entirely contained in the :meth:`action <omni.base.workspace.Workspace.action>` function and calls many of the member functions of the :class:`Workspace <omni.base.workspace.Workspace>` class. In the third step described above, the :meth:`computer <omni.base.computer.computer>` function will be used to repeatedly send a simulation to a calculation function. 

The main loop is designed to be hidden from the user, who is only expected to write the metadata and the most important component of the loop: the calculation functions. Calculation functions should be stored in ``calcs/function_name.py`` and should contain a **single python function with the same name** as the file. This function can call external libraries or local libraries stored in ``calcs`` (typically ``calcs/codes``), but must be named carefully so that the :meth:`compute <omni.base.computer.computer>` function can find it. If the calculation's ``uptype`` flag is set to ``simulation`` then this function will receive a two arguments, namely the ``grofile`` and ``trajfile`` which will point to the structure and trajectory of the slice created in the second step. If the ``uptype`` is ``post``, the the function will receive a copy of the upstream data. It will also pass other ``kwargs`` that specify the features of the calculation found in the ``specs`` sub-dictionary. A typical calculation block from ``calcs/specs/meta.yaml`` is pictured below.

.. code-block :: yaml

	calculations:
	  lipid_abstractor:
	    uptype: simulation
	    slice_name: current
	    group: all
	    collections: all
	    specs:
	      selector:
	        loop:
	          lipid_com:
	            monolayer_cutoff: 1.85
	            resnames: +selectors/resnames_lipid
	            type: com
	          lipid_chol_com:
	            monolayer_cutoff: 1.4
	            resnames: +selectors/resnames_lipid_chol
	            type: com

The calculation is named ``lipid_abstractor`` hence the user must create ``calcs/lipid_abstractor.py`` which contains a function which is also called ``lipid_abstractor``. The calculation dictionary specifies a few key parameters. 

1. Users can request the original simulation trajectory (or "slice") by setting ``uptype: simulation``. This sends the structure and trajectory to the analysis function in ``grofile`` and ``trajfile``. Simulations which only depend on another "upstream" calculation should set ``uptype: post`` and also specify an ``upstream`` variable which lists the names of the previous calculations. See the :ref:`parameter sweeps <sec-parameter-sweeps>` section for an example of how the parameters are specified in a calculation with upstream dependencies.
2. Users must identify a ``slice_name`` and a ``group``, both of which are necessary to uniquely identify a slice specified in the top-level :ref:`slices <sec-slices>` dictionary. 
3. Users must also identify a list of ``collections`` of simulations to apply the calculation. Collections are specified in a top-level dictionary called ``collections`` which is found the metadata file. Multiple collections should be compiled into a list. Note that each collection requested by a calculation must have corresponding slices specified by ``slice_name``. If omnicalc cannot find the corresponding slice or group, it will throw an error. *The collections list is necessary to apply the calculations to your simulations*. Even if you analyze a single simulation, it needs to be in a collection.
4. Specs are optional, but allow the user to set attributes which are passed all the way to the final data output. These attributes make it easy to perform arbitrary parameter sweeps. In the example above, the loop over the ``selector`` parameter sends different distance cutoffs and lipid selections to the calculation function in order to generate a lipid trajectory either with or without cholesterol. 

A few, strict rules
-------------------

The omnicalc design philosophy expects more from the user than a typical software package. The incoming data, metadata, and calculation functions must be written according to the framework specified here and in the other chapters of the documentation. In this way, the authors have selected `convention over configuration <https://en.wikipedia.org/wiki/Convention_over_configuration>`_. This means that omnicalc works with a few, very strict rules. The upshot is that users can prepare metadata that make calculations highly customizable and scalable. New parameter sweeps can be instantiated simply by editing a ``calcs/specs/meta.yaml`` file and running ``make compute``. Note that omnicalc will not perform downstream functions (namely, rendering plots) if you update the metadata without running ``make compute``. You can always use the :meth:`respec <omni.controller.respec>` function to update the workspace with your metadata when making adjustments to your plots.

Calculation functions can be written in a highly modular format so that they can be shared between different data sets. For example, the authors have used the *exact* same calculation codes on both atomistic and coarse-grained simulations despite their radically different naming conventions. This scheme also ensures that the codes are easily extensible to slightly novel use-cases.

When things go wrong
--------------------

Given that omnicalc operates as a framework described above, errors should be interpreted in terms of the position inside the main loop. Whenever you encounter an error, you can find more details about what caused the error by checking the source code. Oftentimes the position within the main loop will tell you what went wrong. Users may also use the ``make look`` utility function to inspect the workspace variable to make sure everything is in order.

.. warning ::
	
	better description of error handling. perhaps an example would be useful.

Utility functions
-----------------

.. warning ::

	controller functions are coming soon

Plotting
--------

Plotting functions can be executed with ``make plot`` or preferably ``make plot <my_plot_script>``, since this function *always* re-makes the plots, in contrast to the ``make compute`` function which will only generate post-processing data once. 

.. note ::

	The ``make compute`` loop is lazy. If it finds the post-processing binaries for a calculation, it won't re-run that calculation. This design has the advantage that users may add new calculations or extend parameter sweeps in the metadata without recalculating anything. The downside is that changing any hard-coded calculation parameters typically requires that the user manually delete the deprecated binaries. These are usually clearly named, so this isn't difficult, but in general the authors recommend adding data rather than deleting it and rerunning the calculation. This preserves the calculation history in case something goes wrong. Once you are ready to plot your data, you can single out a particular set of parameters, even if you swept over many. Omnicalc keeps track of the calculation details (typically given in the ``specs`` subdictionary for a particular calculation), which makes it easy to look up the results of a specific calculation. Since plots are both fast and endlessly customizable, the ``make plot`` command will always regenerate the plot. 

.. warning ::

	Plots have attributes too, so add a link to the note above when they are documented.




