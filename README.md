ComfyUI_block_sweep is a tool to be used with the flux 1 dev block nodes. It will automatically 
generate a set number of images while randomly adjusting the slider values for a chosen seed 
number.

ComfyUI block sweep — randomize sliders, fixed seed, multiple batches.

Each batch has its own:
  • seed
  • image count
  • filename prefix (applied to the SaveImage node)
  • node selection (which of the 6 Flux block nodes to randomize)

Fetches slider min/max/step from /object_info at runtime so ranges
always match whatever the node files declare.

For each image queued, writes a .txt sidecar with the same base name
listing every slider value grouped by node.

Requirements:
    pip install websocket-client requests
    tkinter is included with standard Python on Windows

Place in the same folder as your workflow JSON files.
