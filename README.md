# NB
This is a company specific example of creating ths output, and due to commercial sensitivity, underlying data files cannot be provided.
However, the script provided helps to frame setting up such a process - to save time/resource in compiling updated and formulated PowerPoint insights packs for stakeholders.


# Files and requirements to run process to create building analysis powerpoint

---

## Python code and associated artefacts to produce automated building analysis report (powerpoint)

All Python code is contained in a single .py script.

This file calls the config file that contains 3 parameters that end users must pass:

1. Single building id or list of building ids (company specific reference, but a building polygon is required)
2. Diamater (long diagonal) in feet of the hexbins to apply for analysis across the building xy axis
3. Maximum number of building floors to include (these are selected based upon floors with greatest measurement volumes

---

## The process requires the following support files to produce output:

1. Powerpoint template (found in the Powerpoint_templates folder)
2. Image files for (found in the 'Images' folder.):
	1. Measurements legend - Measurement_colour_scale.png
	2. RSRP and RSSNR legend - RF_colour_scale.png
	3. Hexbin size guide - Internal_hexbin_size_v2.png

---
