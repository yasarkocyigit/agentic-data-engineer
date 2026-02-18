from PIL import Image
import os

def remove_background(input_path, output_path, tolerance=50):
    try:
        img = Image.open(input_path).convert("RGBA")
        datas = img.getdata()
        
        # Sample corners to find background color
        width, height = img.size
        candidates = [
            img.getpixel((0, 0)),
            img.getpixel((width-1, 0)),
            img.getpixel((0, height-1)),
            img.getpixel((width-1, height-1))
        ]
        
        # Most common corner color is likely the background
        from collections import Counter
        bg_color = Counter(candidates).most_common(1)[0][0]
        
        print(f"Detected background color: {bg_color}")
        
        newData = []
        for item in datas:
            # Check difference
            diff = sum(abs(item[i] - bg_color[i]) for i in range(3))
            if diff < tolerance:
                newData.append((255, 255, 255, 0)) # Transparent
            else:
                newData.append(item)
        
        img.putdata(newData)
        img.save(output_path, "PNG")
        print(f"Successfully processed {input_path} -> {output_path}")
        
    except Exception as e:
        print(f"Error processing image: {e}")

if __name__ == "__main__":
    target_file = "web-ui/public/logo.png"
    if os.path.exists(target_file):
        remove_background(target_file, target_file)
    else:
        print(f"File not found: {target_file}")
