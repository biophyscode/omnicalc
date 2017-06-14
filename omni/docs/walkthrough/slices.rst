
.. _sec-slices:

Slices
======

The top-level ``slices`` dictionary in your :ref:`metadata <sec-metadata-basic>` contain instructions for "slicing" a long trajectory into a single file. This has a number of advantages for users who have completed a production run which spans many different parts. The slicing procedure also (crucically) can be used to reassemble molecules which are broken across periodic boundaries, which always happens when you restart your GROMACS simulation.

Each entry in the ``slices`` dictionary should be a formal simulation name. That is, it must be the folder name for a simulation that can be identified via :ref:`paths.yaml <sec-paths>`. Recall that you are free to map these simulation names to more convenient, human readable names later on, using the ``meta`` dictionary or the internal :ref:`variables <sec-variables>` feature. 

Each entry in the ``slices`` dictionary must contain two children. The ``slices`` entry is a dictionary of slice definitions. The keys correspond to slice names which are only used internally to keep track of the slices (note that the calculations retrieve slices by using these names, which do not have to be unique across simulation entries). The slice dictionary must contain a few keys. The ``groups`` key must provide a list of group names which we will describe in a moment. You must also include ``start``, ``end``, and ``skip`` which correspond to simulation times sent to the GROMACS trjconv utility. These should all be floats or integers specified in picoseconds.

The parallel ``groups`` entry should be a dictionary of selection commands which are fed first to the GROMACS ``make_ndx`` command to specify the contents of a particular trajectory slice. In the simplest case, you should include ``all:all`` if you want to retain all molecules in your slice. You may find it economical to make some slices which only contain a part of your system. These names should be added to the ``groups`` key in the slice definition. The following example should make this clear.

.. code-block :: yaml

	slices:
	  membrane-v123:
	    groups: 
	      all: all
	      ions: +selectors/ions
	    slices:
	      current: {'pbc':'mol','groups':['all'],'start':10000,'end':110000,'skip':100}
	      current_ions: {'pbc':'mol','groups':['ions'],'start':10000,'end':110000,'skip':1}


The name of each slice is only used by omnicalc to keep track of that slice later on. For example, when you design calculations, you can run a single calculation over all slices of a particular name. Remember that the names do not have to be unique for each simulation. In the example above, we create two kinds of slices (``current`` and ``current_ions``) which can be accessed by different calculations. The ``current_ions`` slice is sampled at a much higher rate (in this case ``skip=1`` so the frames are written every picosecond) and could be sent to a calculation which only requires the positions of the ions.
