from PIL import Image
import os
from collections import Counter

def remove_background_advanced(input_path, output_path, tolerance=50):
    try:
        img = Image.open(input_path).convert("RGBA")
        datas = img.getdata()
        
        # 1. Analyze colors to find the "background" that isn't transparency
        # Filter out fully transparent pixels
        visible_pixels = [p for p in datas if p[3] > 0]
        
        if not visible_pixels:
            print("Image is fully transparent.")
            return

        # Find most common color (assuming it's the background box)
        most_common_color = Counter(visible_pixels).most_common(1)[0][0]
        print(f"Detected dominant visible color: {most_common_color}")
        
        # Validate if it's a likely background (Gray/White/Black)
        # Check saturation? Or just rely on frequency.
        # If it's a solid box, it will be very frequent.
        
        bg_color = most_common_color
        
        newData = []
        for item in datas:
            # If transparent, keep it
            if item[3] == 0:
                newData.append(item)
                continue
                
            # Check difference from detected background
            diff = sum(abs(item[i] - bg_color[i]) for i in range(3))
            if diff < tolerance:
                 newData.append((255, 255, 255, 0)) # Make Transparent
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
        remove_background_advanced(target_file, target_file)
    else:
        print(f"File not found: {target_file}")
