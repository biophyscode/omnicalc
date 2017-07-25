
.. _sec-metadata-basic:

Metadata
========

The design of omnicalc is motivated by a common problem experienced by the authors. In the course of answering a scientific question, you may write an analysis program that answers *exactly* that question. Once you learn something useful, you might ask follow-up questions that typically require you to add to these scripts, or sweep a particular parameter in some way. After a few iterations, your original calculation code becomes littered with parameter choices that are important to your calculation. The design philosophy behind the omnicalc code is to centralize all of these parameter choices in one place, in a readable format that is easy to expand. We store parameters in one (or many) `yaml <http://www.yaml.org>`_ files. These files constitute the "metadata" for our calculations, and we try our best to separate these metadata from the more generic functions which analyze our simulations. This ensures that your calculations are as modular as possible.

Paths
-----

As a rule, all metadata files must be stored in ``calcs/specs/*.yaml``. You can use any naming scheme you wish, as long as the files are suffixed with ``.yaml``. For the tutorial, we will be using ``meta.yaml``. The yaml files have a particular top-level structure which allows omnicalc to merge them if it finds more than one. This merging is described later. It's useful for the factory.

.. warning :: 

	link to the merging rules later on

Structure
---------

The yaml files are automatically merged into a single dictionary accessible throughout the omnicalc codes. The final result must contain at least the following list of top-level keys. Users with highly complicated settings may wish to put each top-level key in a separate file. If you use the `factory <http://github.com/bradleyrp/factory>`_, it will generate additional yaml files to reflect user input to the front-end GUI, however users may always add additional metadata manually. 

1. **slices**: tells omnicalc how to create seamless trajectory slices
2. **variables**: avoid repeating yourself by defining common terms here
3. **collections**: organize simulations into groups which can be analyzed *en masse*
4. **meta**: alias your possibly clunky simulation names to human-readable ones (particularly useful when making plots)
5. **calculations**: specify a single calculation (or a series) over an arbitrary set of parameters
6. **plots**: specify formats and parameters for making plots

Each of these items will be described in detail in the remainder of the documentation. In section below, we will describe our custom syntax for avoiding repetition in these files. Note that the list above contains protected top-level dictionary keys. You can add extra data to the meta files, but omnicalc will typically only use information in the above-named dictionaries. These dictionaries are intelligently merged in case they are found in separate specification files (that is, any file which matches ``calcs/specs/*yaml``).

.. _sec-variables:

Variables
---------

All of the metadata (or "specification") files found in ``calcs/specs/*yaml`` should be written in the standard yaml format. We have added a single useful tool for avoiding repetition. The entire set of yaml files is read into a single dictionary. Any "leaf" or "child node" of this dictionary which is (1) a string and (2) begins with a plus sign (e.g. ``+lipids``) will trigger a variable lookup. If omnicalc fails to map the string to a variable, it will not throw an error.

Omnicalc will replace the variable name with the value it finds in the top-level ``variables`` dictionary. Since this dictionary can be arbitrarily nested, it will traverse the dictionary using a series of keys separated by a forward slash. 

.. code-block :: yaml

	variables:
	  selectors:
	    ions: (name NA or name CL or name MG or name Cal or name K)
	    cations: (name NA or name MG or name Cal or name K)
	    resnames_lipid: ['POPC','DOPC','DOPS','DOPE']

In the example above, whenever we set a child node to "+selectors/ions" it will be automaticall replaced with a string: ``(name NA or name CL or name MG or name Cal or name K)``. Note that the yaml specification allows pythonic lists, so the ``resnames_lipid`` value would be returned as a proper list. The ``selectors`` in the example above consist of CHARMM-style selection strings which are later sent to the MDAnalysis package for selecting a subset of our simulation. Storing these strings as variables means that you don't have to repeat them elsewhere, and you can change them at a single location which might affect many downstream calculations.

The authors have found the ``variables`` dictionary to be useful not only for storing selection commands, but also timing information for trajectory slices, common mathematical definitions, and extraneous settings. This feature keeps the metadata simple and provides an additional layer of abstraction.

.. warning :: 

	please confirm no error on variable lookup
