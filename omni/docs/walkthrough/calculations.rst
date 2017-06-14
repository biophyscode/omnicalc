
Calculations
============

In the previous section (:ref:`run time <sec-runtime>`) we outlined the main execution loop and the logic by which omnicalc links calculation parameters (i.e. metadata) with simulation trajectories and calculation functions. In this section we will describe the structure of a calculation function. By the end of this section, users should have all of the information they need to perform simple calculations, however, a wide variety of extensions and features are described in the remaining chapters.

An example calculation
----------------------

The following code describes a simple example function for the "undulations" calculation found in ``calcs/undulations.py``. This function is designed to read the results of a previous calculation called ``lipid_abstractor`` which reduces a bilayer simulation to a set of lipid centers-of-mass for further calculation. This example covers almost all of the features of the omnicalc workflow, which we will address in sequence.

.. literalinclude :: undulations_example.py
  :tab-width: 2
  :emphasize-lines: 19-24,41-48

Using external codes
~~~~~~~~~~~~~~~~~~~~

For starters, you might notice that there is almost no formal computation visible in this function. Almost all of the "work" is performed by the ``makemesh_regular`` function imported from ``codes.mesh``. Users may wish to embed the computation directly in this function, but they are free to import any modules they wish. Besides allowing local imports from e.g. ``calcs/codes``, users may also import global packages. In this case, we use the `joblib <https://pythonhosted.org/joblib/>`_ package to parrallelize this code using shared memory. We also use omnicalc's built-in ``framelooper`` generator to iterate over the number of frames in our simulation using a status bar and a timer.

.. _sec-upstream:

Requesting upstream data
~~~~~~~~~~~~~~~~~~~~~~~~

Since this function depends on an "upstream" ``lipid_abstractor`` computation, omnicalc automatically sends the data in ``kwargs['upstream']['lipid_abstractor']``. It is possible to draw from multiple upstream calculations. Users specify the upstream dependencies inside of the calculations dictionary in the metadata. To import the ``lipid_abstractor`` data, the user uses the ``upstream`` keyword according to the following example. (Recall that ``group``, ``slice_name``, and ``collections`` are required for all calculation dictionaries).

.. code-block :: yaml

  undulations:
    uptype: post
    group: lipids
    slice_name: +slices/steady16
    collections:
      - all
    specs:
      grid_spacing: 0.5
      upstream: lipid_abstractor

The ``upstream`` key above can point to either an item or a list (see :ref:`parameter sweeps <sec-parameter-sweeps>`), but these items must be the names of *other* calculations. Omnicalc will figure out the correct execution order for you. The ``uptype: post`` flag tells omnicalc not to load the simulation trajectory directly. If you use ``uptype: simulation``, then omnicalc will send along the structure and trajectory files as arguments named ``grofile`` and ``trajfile``. These arguments can be passsed directly to the excellent `MDAnalysis package <http://www.mdanalysis.org/>`_, which is equipped to read the GROMACS binary trajectory files. Note that you can request ``upstream`` calculations even when you set ``uptype: simulation``, in the event that you want to refer back to the original simulation trajectory on a downstream step.

All of the upstream data, file names, and specs are passed via ``kwargs``. It is the users's job to unpack them for the calculation.

Packaging the data
~~~~~~~~~~~~~~~~~~

After the body of the calculation function, users will be ready to "save" the data. All calculation functions must return data in a specific format so that omnicalc can save it for downstream calculations or plotting functions. Data should be stored in either a results dictionary or an attributes dictionary. 

The results dictionary can **only** contain ``numpy`` array data. Most data --- even multidimensional lists with different lengths, dictionaries, etc, can be saved as a numpy array. This restriction allows omnicalc to use the excellent `h5py <http://www.h5py.org/>`_ package to save the data in a hardware agnostic, efficient, binary format.

.. note :: 

	You can save highly heterogeneous data (e.g. nested dictionaries) in the numpy format by using packages like `JSON <http://www.json.org/>`_, or `YAML <http://yaml.org/>`_, to turn them into a string, which can be saved. This can be done with numpy as follows: ``numpy.string_(json.dumps(ugly_data))``.

The attributes dictionary is called ``attrs`` and has a few strict requirements which are designed to make it easy for omnicalc to retrieve data. In short, the ``attrs`` dictionary should contain any parameters which describe the calculation and distinguish it from others, particularly those in a parameter sweep. Specifications are stored in the calculations dictionary (in the metadata) under the ``specs`` key. Since these parameters are essential to identify the calculation after it is complete, omnicalc will throw an error of the user fails to pass the ``specs`` on to the ``attrs`` dictionary. In the example above, you can see  that we have passed along the ``grid_spacing`` parameter. You can also add other parameters to ``attrs`` to further label the data. 

One of the most distinctive features of omnicalc is that the software is designed to collect parameters in the metadata files (in ``calcs/specs/*.yaml``) so that you don't need to "hard code" them in your analysis functions. Inevitably, you will hard code some of these parameters, and later realize that they *are* in fact, paramters which you want to vary. If you export the hard-coded parameters in ``attrs``, then you can later add them to the metadata files (and sweep over them, for example), without causing a naming conflict or deleting the original calculation.

Where the data are stored
~~~~~~~~~~~~~~~~~~~~~~~~~

Each calculation produces two files: a ``dat`` file written in ``hdf5`` as described above, and a specs file containing a text-formatted python dictionary given by ``attrs``. These files are stored in the path given by the ``work.paths['post_data_spot']`` variable and specified in the configuration. The file names are nearly identical to the slice names (see: :ref:`naming slices <sec-slice-names>`) with two small changes. As with the slice names, they begin with the prefixed simulation name defined by ``prefixer`` in the configuration. This is followed by the calculation name defined in the metadata. The only other difference between a slice file name and a calculation file name is that the calculations have a suffix which contains an index number. This index distinguishes distinct calculations from each other. These differences are encoded in the corresponding ``spec`` file, which contains the ``attrs`` defined by the user. 

This naming scheme allows the user to produce an *arbitrary* number of calculations with different parameters without using bloated file names. The parameters are stored in the ``spec`` file, which is studied by omnicalc to figure out which ``dat`` file to open, when you make plots or access the data later on. The index on the spec file is equivalent to a foreign key in a database. The example in the upcoming section uses the following names, where the index is ``n0``.

.. code-block :: bash

  v532.50000-100000-100.lipid_mesh.n0.dat
  v532.50000-100000-100.lipid_mesh.n0.spec

.. _sec-parameter-sweeps:

Parameter sweeps
~~~~~~~~~~~~~~~~

The example ``undulations`` function above refers to the ``lipid_abstractor`` data without further specification. In the event that your upstream data contains a parameter sweep, you can also perform the sweep over the downstream data. The following example describes a calculation called ``lipid_mesh`` which uses two different lipid selectors (one which contains cholesterol, and one which doesn't). Using the ``loop`` keyword in the ``specs`` will trigger a parameter sweep.

.. code-block :: yaml

  lipid_mesh:
    uptype: post
    slice_name: current
    collections:
      - all
    specs:
      upstream:
        lipid_abstractor:
          selector:
            loop:
              - lipid_com
              - lipid_chol_com

Any downstream steps must either perform the same parameter sweep, or **select uniquely-identifying parameters** for the upstream step in order to import that data. In both cases, the selection is made inside the ``upstream`` dictionary in ``specs``. If there are no parameters, then the ``upstream`` item can be a list (or a single) item. If you need to select parameters, or perform the sweep above, then ``upstream`` should be a list of dictionaries, each of which contains the ``specs`` section from the upstream calculation. 

The example above mimics a parameter sweep that must have also happened in the ``lipid_abstractor`` calculation. If users only wish to use one parameter for the ``lipid_mesh`` calculation, they would still have to select it, using the following notation. In the following example, we choose to include the cholesterol molecules via the ``selector`` spec.

.. code-block :: yaml

  lipid_mesh:
    uptype: post
    slice_name: current
    collections:
      - all
    specs:
      upstream:
        lipid_abstractor:
          selector: lipid_chol_com

By using ``loop``, ``upstream``, and ``specs``, users can develop highly efficient calculation pipelines. 

.. note :: 

  If you trigger a parameter sweep by using the keyword ``loop`` as per the example above, then the calculation will loop over all of the subsequent lists. You can specify the same parameter sweep in the plots section, or you can omit the specs entirely. In both cases, the :meth:`plotloader <omni.base.store.plotloader>` function will load all of the data you require. You can whittle this down by using a ``specs`` sub-dictionary to select exactly which data goes to the plot functions.

